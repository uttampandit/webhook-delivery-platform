package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/receiver"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("receiver stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	settings, err := config.Load(config.ServiceReceiver)
	if err != nil {
		return err
	}
	cfg, err := pgxpool.ParseConfig(settings.DatabaseURL())
	if err != nil {
		return errors.New("could not configure receiver database")
	}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = settings.PostgreSQLStatementTimeout()
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return errors.New("could not create receiver database pool")
	}
	defer pool.Close()
	h := receiver.New(store.NewDemoRepository(pool, settings.ReceiverCode()), signing.Auth{Keys: settings}, settings.DemoControlToken(), settings.DBStatementTimeout())
	webhook, control := newServers(settings, h)
	wl, err := net.Listen("tcp", webhook.Addr)
	if err != nil {
		return errors.New("could not listen on receiver webhook address")
	}
	defer wl.Close()
	cl, err := net.Listen("tcp", control.Addr)
	if err != nil {
		return errors.New("could not listen on receiver control address")
	}
	defer cl.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errs := make(chan error, 2)
	go func() { errs <- webhook.Serve(wl) }()
	go func() { errs <- control.Serve(cl) }()
	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = errors.New("receiver listener failed")
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Shut both listeners concurrently so neither accepts work during shutdown.
	done := make(chan error, 2)
	go func() { done <- webhook.Shutdown(shutdown) }()
	go func() { done <- control.Shutdown(shutdown) }()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			webhook.Close()
			control.Close()
			return errors.New("receiver shutdown failed")
		}
	}
	return serveErr
}
func newServers(s config.Settings, h *receiver.Handler) (*http.Server, *http.Server) {
	webhook, control := ":8081", ":9091"
	if s.RunMode() == config.RunModeHost {
		webhook, control = "127.0.0.1:8082", "127.0.0.1:9092"
		if s.ReceiverCode() == "notification" {
			webhook, control = "127.0.0.1:8083", "127.0.0.1:9093"
		}
	}
	server := func(addr string, handler http.Handler) *http.Server {
		return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 * 1024}
	}
	return server(webhook, h.Webhook()), server(control, h.Control())
}
