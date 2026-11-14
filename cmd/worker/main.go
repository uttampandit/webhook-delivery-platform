package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
)

func main() {
	telemetry.ConfigureJSONLogger("worker")
	if err := run(); err != nil {
		slog.Error("worker stopped", "error_code", "worker_startup_failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	settings, err := config.Load(config.ServiceWorker)
	if err != nil {
		return err
	}
	cfg, err := pgxpool.ParseConfig(settings.DatabaseURL())
	if err != nil {
		return errors.New("could not configure worker database")
	}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = settings.PostgreSQLStatementTimeout()
	cfg.MaxConns = int32(settings.WorkerConcurrency() + 2)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return errors.New("could not create worker database pool")
	}
	defer pool.Close()
	metrics, err := telemetry.NewMetrics()
	if err != nil {
		return errors.New("could not initialize runtime metrics")
	}
	metricsPoolConfig, err := pgxpool.ParseConfig(settings.DatabaseURL())
	if err != nil {
		return errors.New("could not configure worker metrics database pool")
	}
	metricsPoolConfig.MaxConns = 1
	if metricsPoolConfig.ConnConfig.RuntimeParams == nil {
		metricsPoolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	metricsPoolConfig.ConnConfig.RuntimeParams["statement_timeout"] = "1s"
	metricsPool, err := pgxpool.NewWithConfig(context.Background(), metricsPoolConfig)
	if err != nil {
		return errors.New("could not create worker metrics database pool")
	}
	defer metricsPool.Close()
	claims, err := store.NewClaimRepository(pool, settings.LeaseDuration(), settings.DBStatementTimeout())
	if err != nil {
		return err
	}
	finalize, err := store.NewFinalizationRepository(pool, settings.DBStatementTimeout(), delivery.Policy{})
	if err != nil {
		return err
	}
	sender, err := delivery.NewHTTPSender(signing.Auth{Keys: settings}, settings.HTTPTimeout())
	if err != nil {
		return err
	}
	defer sender.Close()
	worker, err := delivery.NewWorker(claims, finalize, sender, delivery.WorkerOptions{Concurrency: settings.WorkerConcurrency(), PollInterval: settings.WorkerPollInterval(), FinalizeTimeout: settings.DBStatementTimeout(), ShutdownTimeout: 9 * time.Second, Metrics: metrics})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	recovery, err := store.NewRecoveryRepository(pool, settings.DBStatementTimeout(), delivery.Policy{})
	if err != nil {
		return err
	}
	metricsAddress := ":9090"
	if settings.RunMode() == config.RunModeHost {
		metricsAddress = "127.0.0.1:9091"
	}
	metricsServer := &http.Server{Addr: metricsAddress, Handler: metrics.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	metricsErrors := make(chan error, 1)
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsErrors <- err
			slog.Error("worker stopped", "error_code", "metrics_listener_failed")
			stop()
		}
	}()
	go metrics.RunGaugeCollector(ctx, 15*time.Second, time.Second, store.NewQueueMetricsRepository(metricsPool, settings.Registry().Tenant().ID).ReadQueueMetrics)
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		delivery.RunReaper(ctx, recovery, settings.ReaperInterval(), settings.DBStatementTimeout(), metrics)
	}()
	err = worker.Run(ctx)
	stop()
	<-reaperDone
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if shutdownErr := metricsServer.Shutdown(shutdown); err == nil && shutdownErr != nil {
		return errors.New("worker metrics listener shutdown failed")
	}
	select {
	case listenerErr := <-metricsErrors:
		if listenerErr != nil {
			return errors.New("worker metrics listener failed")
		}
	default:
	}
	return err
}
