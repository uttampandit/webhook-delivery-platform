package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/democtl"
)

func main() { os.Exit(run()) }
func run() int {
	settings, err := config.Load(config.ServiceDemoctl)
	if err != nil {
		fmt.Fprintln(os.Stderr, "democtl configuration invalid; check local environment")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := democtl.New(settings, democtl.DefaultEndpoints(settings.RunMode()))
	return client.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)
}
