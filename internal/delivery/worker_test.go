package delivery

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type claimFunc func(context.Context, int) ([]ClaimedDelivery, error)

func (f claimFunc) ClaimDue(ctx context.Context, n int) ([]ClaimedDelivery, error) { return f(ctx, n) }

type sendFunc func(context.Context, ClaimedDelivery) SendResult

func (f sendFunc) Send(ctx context.Context, c ClaimedDelivery) SendResult { return f(ctx, c) }

type finalizeFunc func(context.Context, ClaimedDelivery, SendResult) (FinalizationResult, error)

func (f finalizeFunc) Finalize(ctx context.Context, c ClaimedDelivery, r SendResult) (FinalizationResult, error) {
	return f(ctx, c, r)
}
func TestWorkerCapacityAndFreshShutdownContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	next := 0
	claimed := 0
	started := make(chan struct{}, 4)
	finalized := make(chan struct{}, 4)
	claims := claimFunc(func(ctx context.Context, n int) ([]ClaimedDelivery, error) {
		mu.Lock()
		defer mu.Unlock()
		if n < 1 || n > 4 {
			t.Error("invalid free slots")
		}
		out := []ClaimedDelivery{}
		for len(out) < n && next < 8 {
			next++
			out = append(out, ClaimedDelivery{})
			claimed++
		}
		return out, nil
	})
	sender := sendFunc(func(ctx context.Context, c ClaimedDelivery) SendResult {
		started <- struct{}{}
		<-ctx.Done()
		return SendResult{Outcome: Unconfirmed, ErrorCode: "canceled"}
	})
	finalizer := finalizeFunc(func(ctx context.Context, c ClaimedDelivery, r SendResult) (FinalizationResult, error) {
		if ctx.Err() != nil {
			t.Error("cleanup inherited canceled parent")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("cleanup lacks deadline")
		}
		if r.ErrorCode != "canceled" {
			t.Error("missing canceled result")
		}
		finalized <- struct{}{}
		return FinalizationResult{}, nil
	})
	w, err := NewWorker(claims, finalizer, sender, WorkerOptions{4, time.Millisecond, time.Second, time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("did not fill capacity")
		}
	}
	mu.Lock()
	if claimed != 4 {
		t.Error("prefetched beyond active capacity", claimed)
	}
	mu.Unlock()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown stuck")
	}
	if len(finalized) != 4 {
		t.Fatal("canceled attempts not finalized")
	}
}
func TestWorkerNeverSendsAfterClaimError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sends atomic.Int32
	claims := claimFunc(func(context.Context, int) ([]ClaimedDelivery, error) {
		cancel()
		return []ClaimedDelivery{{}}, errors.New("ambiguous commit")
	})
	w, err := NewWorker(claims, finalizeFunc(func(context.Context, ClaimedDelivery, SendResult) (FinalizationResult, error) {
		t.Error("unexpected finalization")
		return FinalizationResult{}, nil
	}), sendFunc(func(context.Context, ClaimedDelivery) SendResult { sends.Add(1); return SendResult{} }), WorkerOptions{4, time.Second, time.Second, time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Run(ctx); err != nil || sends.Load() != 0 {
		t.Fatal("unconfirmed claim sent", err)
	}
}

func TestWorkerRefillsOnlyNewlyFreeSlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := make(chan int, 16)
	started := make(chan struct{}, 8)
	permit := make(chan struct{}, 1)
	next := 0
	claims := claimFunc(func(ctx context.Context, n int) ([]ClaimedDelivery, error) {
		requests <- n
		out := []ClaimedDelivery{}
		for len(out) < n && next < 8 {
			next++
			out = append(out, ClaimedDelivery{})
		}
		return out, nil
	})
	sender := sendFunc(func(ctx context.Context, c ClaimedDelivery) SendResult {
		started <- struct{}{}
		select {
		case <-permit:
		case <-ctx.Done():
		}
		return SendResult{Outcome: Unconfirmed, ErrorCode: "canceled"}
	})
	finalizer := finalizeFunc(func(context.Context, ClaimedDelivery, SendResult) (FinalizationResult, error) {
		return FinalizationResult{}, nil
	})
	worker, err := NewWorker(claims, finalizer, sender, WorkerOptions{4, time.Millisecond, time.Second, time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	await := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("worker progress deadline")
		}
	}
	for i := 0; i < 4; i++ {
		await(started)
	}
	if n := <-requests; n != 4 {
		t.Fatal("initial slots", n)
	}
	permit <- struct{}{}
	await(started)
	if n := <-requests; n != 1 {
		t.Fatal("refill claimed more than newly free slot", n)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown deadline")
	}
}
