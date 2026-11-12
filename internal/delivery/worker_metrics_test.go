package delivery

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
)

func TestWorkerRecordsStaleFinalizationFenceRejection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics, err := telemetry.NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	claimCalls := 0
	claims := claimFunc(func(context.Context, int) ([]ClaimedDelivery, error) {
		claimCalls++
		if claimCalls == 1 {
			return []ClaimedDelivery{{StartedAt: time.Now(), ScheduledAt: time.Now()}}, nil
		}
		cancel()
		return nil, nil
	})
	worker, err := NewWorker(claims, finalizeFunc(func(context.Context, ClaimedDelivery, SendResult) (FinalizationResult, error) {
		return FinalizationResult{}, ErrStaleFinalization
	}), sendFunc(func(context.Context, ClaimedDelivery) SendResult {
		return SendResult{Outcome: Acknowledged}
	}), WorkerOptions{Concurrency: 1, PollInterval: time.Millisecond, FinalizeTimeout: time.Second, ShutdownTimeout: time.Second, Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not finish after stale finalization")
	}
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	got := recorder.Body.String()
	if !strings.Contains(got, "webhook_stale_finalization_total 1") {
		t.Fatalf("stale finalization counter did not advance: %s", got)
	}
	if strings.Contains(got, "webhook_attempts_total") || strings.Contains(got, `webhook_deliveries_total{result="succeeded"}`) {
		t.Fatalf("uncommitted finalization was counted as durable outcome: %s", got)
	}
}

func TestWorkerCountsOnlyCommittedAttemptAndDeliveryOutcomes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics, err := telemetry.NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	created := time.Now().Add(-time.Second)
	claimCalls := 0
	claims := claimFunc(func(context.Context, int) ([]ClaimedDelivery, error) {
		claimCalls++
		if claimCalls == 1 {
			return []ClaimedDelivery{{StartedAt: time.Now(), ScheduledAt: created, EventCreatedAt: created, CycleNo: 2}}, nil
		}
		cancel()
		return nil, nil
	})
	worker, err := NewWorker(claims, finalizeFunc(func(context.Context, ClaimedDelivery, SendResult) (FinalizationResult, error) {
		return FinalizationResult{Outcome: Acknowledged, Status: "succeeded", DeliveryLatency: time.Second, DeliveryLatencyObserved: true}, nil
	}), sendFunc(func(context.Context, ClaimedDelivery) SendResult {
		return SendResult{Outcome: Acknowledged}
	}), WorkerOptions{Concurrency: 1, PollInterval: time.Millisecond, FinalizeTimeout: time.Second, ShutdownTimeout: time.Second, Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not finish after successful finalization")
	}
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	got := recorder.Body.String()
	for _, want := range []string{`webhook_attempts_total{outcome="acknowledged"} 1`, `webhook_deliveries_total{result="succeeded"} 1`, `webhook_delivery_latency_seconds_count 1`} {
		if !strings.Contains(got, want) {
			t.Fatalf("committed outcome missing %q:\n%s", want, got)
		}
	}
}
