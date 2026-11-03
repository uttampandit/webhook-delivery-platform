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
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

const seedDeadline = 30 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("seed stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	settings, err := config.Load(config.ServiceSeed)
	if err != nil {
		return err
	}

	poolConfig, err := pgxpool.ParseConfig(settings.DatabaseURL())
	if err != nil {
		return errors.New("DATABASE_URL could not configure a PostgreSQL connection")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return errors.New("could not create the PostgreSQL connection pool")
	}
	defer pool.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, seedDeadline)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		return errors.New("could not connect to PostgreSQL; verify DATABASE_URL and that the database is ready")
	}
	if err := store.Seed(ctx, pool, settings.Registry()); err != nil {
		return err
	}
	slog.Info("seed complete")
	return nil
}
