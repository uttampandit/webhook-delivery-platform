package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/uttampandit/webhook-delivery-platform/internal/benchmark"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

func main() { os.Exit(run()) }

func run() int {
	options, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	settings, err := config.Load(config.ServiceBenchmark)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchmark configuration invalid; check local PostgreSQL and required API tokens")
		return 2
	}
	endpoints, err := benchmark.DefaultEndpoints(settings.RunMode())
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	runner, err := benchmark.NewRunner(settings, endpoints, options, "artifacts/bench", os.Getenv("BENCHMARK_HOST_METADATA"), os.Getenv("BENCHMARK_RUNTIME_METADATA"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, reportPath, err := runner.Run(ctx)
	if reportPath != "" {
		encoded, encodeErr := json.Marshal(struct {
			RunID                string `json:"run_id"`
			Status               string `json:"status"`
			AcceptedUniqueEvents int    `json:"accepted_unique_events"`
			Deliveries           int    `json:"deliveries"`
			Report               string `json:"report"`
		}{report.RunID, report.Status, report.Counts.AcceptedUniqueEvents, report.Counts.Deliveries, reportPath})
		if encodeErr == nil {
			fmt.Fprintln(os.Stdout, string(encoded))
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}
	return 0
}

func parseOptions(args []string) (benchmark.Options, error) {
	options := benchmark.Options{
		Events: benchmark.DefaultEvents, ProducerConcurrency: benchmark.DefaultConcurrency,
		PayloadBytes: benchmark.DefaultPayloadBytes, Timeout: benchmark.DefaultTimeout,
	}
	flags := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.IntVar(&options.Events, "events", options.Events, "finite number of unique ingest requests")
	flags.IntVar(&options.ProducerConcurrency, "producer-concurrency", options.ProducerConcurrency, "bounded concurrent ingest producers")
	flags.IntVar(&options.PayloadBytes, "payload-bytes", options.PayloadBytes, "whole serialized ingest request byte target")
	flags.DurationVar(&options.Timeout, "timeout", options.Timeout, "whole-run timeout including drain")
	flags.StringVar(&options.RunID, "run-id", "", "safe unique artifact and workload identifier")
	seen := map[string]bool{}
	for index := 0; index < len(args); index++ {
		token := args[index]
		if !strings.HasPrefix(token, "-") {
			return options, errors.New("benchmark accepts named flags only")
		}
		name, _, hasValue := strings.Cut(strings.TrimLeft(token, "-"), "=")
		if name == "" || flags.Lookup(name) == nil || seen[name] {
			return options, errors.New("benchmark flags are invalid or duplicated")
		}
		seen[name] = true
		if !hasValue {
			index++
			if index >= len(args) || strings.HasPrefix(args[index], "-") {
				return options, errors.New("benchmark flag value is missing")
			}
		}
	}
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return options, errors.New("benchmark flags are invalid")
	}
	if err := benchmark.ValidateOptions(options); err != nil {
		return options, err
	}
	options.Command = append([]string(nil), args...)
	return options, nil
}
