package main

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/api"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
)

const shutdownPeriod = 5 * time.Second

func main() {
	telemetry.ConfigureJSONLogger("api")
	if err := run(); err != nil {
		slog.Error("api stopped", "error_code", "api_startup_failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	settings, err := config.Load(config.ServiceAPI)
	if err != nil {
		return err
	}
	poolConfig, err := pgxpool.ParseConfig(settings.DatabaseURL())
	if err != nil {
		return errors.New("DATABASE_URL could not configure a PostgreSQL connection")
	}
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = settings.PostgreSQLStatementTimeout()
	poolConfig.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return errors.New("could not create the PostgreSQL connection pool")
	}
	defer pool.Close()
	metrics, err := telemetry.NewMetrics()
	if err != nil {
		return errors.New("could not initialize runtime metrics")
	}
	metricsPoolConfig, err := pgxpool.ParseConfig(settings.DatabaseURL())
	if err != nil {
		return errors.New("could not configure API metrics database pool")
	}
	metricsPoolConfig.MaxConns = 1
	if metricsPoolConfig.ConnConfig.RuntimeParams == nil {
		metricsPoolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	metricsPoolConfig.ConnConfig.RuntimeParams["statement_timeout"] = "1s"
	metricsPool, err := pgxpool.NewWithConfig(context.Background(), metricsPoolConfig)
	if err != nil {
		return errors.New("could not create API metrics database pool")
	}
	defer metricsPool.Close()

	server := newHTTPServer(settings, store.NewAPIReadRepository(pool), metrics)
	metricsServer := newMetricsServer(settings, metrics.Handler())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go metrics.RunGaugeCollector(ctx, 15*time.Second, time.Second, store.NewQueueMetricsRepository(metricsPool, settings.Registry().Tenant().ID).ReadQueueMetrics)

	serverErrors := make(chan error, 2)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()
	go func() { serverErrors <- metricsServer.ListenAndServe() }()

	select {
	case err := <-serverErrors:
		stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownPeriod)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		_ = metricsServer.Shutdown(shutdownCtx)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("API or metrics listener failed")
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownPeriod)
		defer cancel()
		apiErr := server.Shutdown(shutdownCtx)
		metricsErr := metricsServer.Shutdown(shutdownCtx)
		if apiErr != nil || metricsErr != nil {
			return errors.New("API shutdown failed")
		}
		return nil
	}
}

func newHTTPServer(settings config.Settings, repository api.Repository, metrics *telemetry.Metrics) *http.Server {
	address := ":8080"
	if settings.RunMode() == config.RunModeHost {
		address = "127.0.0.1:8080"
	}
	return &http.Server{
		Addr:              address,
		Handler:           api.WithStaticUI(api.NewHandlerWithMetrics(settings, repository, metrics), staticFiles()),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func staticFiles() fs.FS {
	directory := os.Getenv("WEB_STATIC_DIR")
	if directory == "" {
		directory = filepath.Join("web", "dist")
	}
	return os.DirFS(directory)
}

func newMetricsServer(settings config.Settings, handler http.Handler) *http.Server {
	address := ":9090"
	if settings.RunMode() == config.RunModeHost {
		address = "127.0.0.1:9090"
	}
	return &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
}
