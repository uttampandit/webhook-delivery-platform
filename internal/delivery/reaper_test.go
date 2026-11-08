package delivery

import (
	"context"
	"testing"
	"time"
)

type recoverFunc func(context.Context) (int, error)

func (f recoverFunc) RecoverExpired(ctx context.Context) (int, error) { return f(ctx) }

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
	w, e := NewWorker(claims, f, sender, WorkerOptions{1, time.Millisecond, time.Second, time.Second})
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
