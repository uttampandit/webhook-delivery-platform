package delivery

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
)

type recoverFunc func(context.Context) (int, error)

func (f recoverFunc) RecoverExpired(ctx context.Context) (int, error) { return f(ctx) }

type detailedRecoverFunc func(context.Context) (int, int, error)

func (f detailedRecoverFunc) RecoverExpired(ctx context.Context) (int, error) {
	recovered, _, err := f(ctx)
	return recovered, err
}
func (f detailedRecoverFunc) RecoverExpiredDetailed(ctx context.Context) (int, int, error) {
	return f(ctx)
}

func TestReaperCountsCommittedRecoveryResults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	metrics, err := telemetry.NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunReaper(ctx, detailedRecoverFunc(func(context.Context) (int, int, error) {
			cancel()
			return 3, 1, nil
		}), time.Second, metrics)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reaper did not stop after committed recovery")
	}
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	got := recorder.Body.String()
	for _, want := range []string{`webhook_attempts_total{outcome="unconfirmed"} 3`, `webhook_deliveries_total{result="dead"} 1`} {
		if !strings.Contains(got, want) {
			t.Fatalf("recovery metrics missing %q:\n%s", want, got)
		}
	}
}

func TestReaperWhileWorkerBusyAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 1)
	claims := claimFunc(func(context.Context, int) ([]ClaimedDelivery, error) { return []ClaimedDelivery{{}}, nil })
	sender := sendFunc(func(ctx context.Context, _ ClaimedDelivery) SendResult {
		started <- struct{}{}
		<-ctx.Done()
		return SendResult{Outcome: Unconfirmed, ErrorCode: "canceled"}
	})
	f := finalizeFunc(func(context.Context, ClaimedDelivery, SendResult) (FinalizationResult, error) {
		return FinalizationResult{}, nil
	})
	w, e := NewWorker(claims, f, sender, WorkerOptions{Concurrency: 1, PollInterval: time.Millisecond, FinalizeTimeout: time.Second, ShutdownTimeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker not busy")
	}
	reaped := make(chan struct{}, 1)
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		RunReaper(ctx, recoverFunc(func(work context.Context) (int, error) {
			if _, ok := work.Deadline(); !ok {
				t.Error("missing independent deadline")
			}
			reaped <- struct{}{}
			<-work.Done()
			return 0, work.Err()
		}), 2*time.Second)
	}()
	select {
	case <-reaped:
	case <-time.After(2 * time.Second):
		t.Fatal("busy slots blocked recovery")
	}
	cancel()
	select {
	case <-reaperDone:
	case <-time.After(time.Second):
		t.Fatal("reaper shutdown blocked")
	}
	select {
	case e := <-workerDone:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker shutdown blocked")
	}
}
