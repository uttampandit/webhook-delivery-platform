package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
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
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return errors.New("could not create worker database pool")
	}
	defer pool.Close()
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
	worker, err := delivery.NewWorker(claims, finalize, sender, delivery.WorkerOptions{Concurrency: settings.WorkerConcurrency(), PollInterval: settings.WorkerPollInterval(), FinalizeTimeout: settings.DBStatementTimeout(), ShutdownTimeout: 9 * time.Second})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	recovery, err := store.NewRecoveryRepository(pool, settings.DBStatementTimeout(), delivery.Policy{})
	if err != nil {
		return err
	}
	reaperDone := make(chan struct{})
	go func() { defer close(reaperDone); delivery.RunReaper(ctx, recovery, settings.DBStatementTimeout()) }()
	err = worker.Run(ctx)
	stop()
	<-reaperDone
	return err
}
