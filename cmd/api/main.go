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
	"github.com/uttampandit/webhook-delivery-platform/internal/api"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

const shutdownPeriod = 5 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("api stopped", "error", err)
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
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return errors.New("could not create the PostgreSQL connection pool")
	}
	defer pool.Close()

	server := newHTTPServer(settings, store.NewAPIReadRepository(pool))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownPeriod)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

func newHTTPServer(settings config.Settings, repository api.Repository) *http.Server {
	address := ":8080"
	if settings.RunMode() == config.RunModeHost {
		address = "127.0.0.1:8080"
	}
	return &http.Server{
		Addr:              address,
		Handler:           api.NewHandler(settings, repository),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
