package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
)

func main() {
	telemetry.ConfigureJSONLogger("migrate")
	if err := run(os.Args[1:]); err != nil {
		slog.Error("migrate stopped", "error_code", "migration_failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	dir := flags.String("dir", "migrations", "directory containing numbered SQL migrations")
	database := flags.String("database", "", "optional database name override for DATABASE_URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("DATABASE_URL is invalid")
	}
	config.MaxConns = 2
	if *database != "" {
		config.ConnConfig.Database = *database
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return fmt.Errorf("could not configure PostgreSQL connection")
	}
	defer pool.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := store.Apply(ctx, pool, *dir); err != nil {
		// Driver connection errors can contain connection settings. Keep CLI
		// output useful without risking a credential in a diagnostic.
		return fmt.Errorf("migration failed; check PostgreSQL availability, DATABASE_URL, and the migration SQL files")
	}
	fmt.Fprintln(os.Stdout, "migrations are up to date")
	return nil
}
