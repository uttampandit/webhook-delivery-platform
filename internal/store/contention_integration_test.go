//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
)

func TestManyClaimersOneDeliveryAndReplayFencePostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, claims, ids := claimFixture(t, ctx, 1)
	type result struct {
		claims []delivery.ClaimedDelivery
		err    error
	}
	done := make(chan result, 12)
	start := make(chan struct{})
	for i := 0; i < 12; i++ {
		go func() { <-start; c, e := claims.ClaimDue(ctx, 1); done <- result{c, e} }()
	}
	close(start)
	var winner delivery.ClaimedDelivery
	total := 0
	for i := 0; i < 12; i++ {
		res := <-done
		if res.err != nil {
			t.Fatal(res.err)
		}
		total += len(res.claims)
		if len(res.claims) > 0 {
			winner = res.claims[0]
		}
	}
	if total != 1 || winner.DeliveryID != ids[0] || winner.Generation != 1 || winner.AttemptNo != 1 {
		t.Fatalf("claims=%d generation=%d attempt=%d", total, winner.Generation, winner.AttemptNo)
	}
	finalizer, e := NewFinalizationRepository(pool, time.Second, delivery.Policy{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = finalizer.Finalize(ctx, winner, sendStatus(400)); e != nil {
		t.Fatal(e)
	}
	if _, e = NewReplayRepository(pool).Replay(ctx, config.TenantID, winner.DeliveryID, "next-cycle", "local-operator"); e != nil {
		t.Fatal(e)
	}
	current := mustClaim(t, ctx, claims, 1)[0]
	if current.CycleNo != 2 || current.CycleAttemptNo != 1 || current.AttemptNo != 2 || current.Generation <= winner.Generation || current.ClaimToken == winner.ClaimToken {
		t.Fatal("replay reused claim identity")
	}
	before := finalSnapshot(t, ctx, pool, current.DeliveryID)
	if _, e = finalizer.Finalize(ctx, winner, sendStatus(204)); e != ErrStaleClaim {
		t.Fatal("old-cycle claim accepted", e)
	}
	if before != finalSnapshot(t, ctx, pool, current.DeliveryID) {
		t.Fatal("old-cycle claim changed current delivery/history")
	}
	if _, e = finalizer.Finalize(ctx, current, sendStatus(204)); e != nil {
		t.Fatal(e)
	}
	var attempts, oldFailure, newSuccess int
	e = pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE cycle_no=1 AND attempt_no=1 AND outcome='http_failure'),count(*) FILTER(WHERE cycle_no=2 AND attempt_no=2 AND outcome='acknowledged') FROM attempts WHERE delivery_id=$1`, current.DeliveryID).Scan(&attempts, &oldFailure, &newSuccess)
	if e != nil || attempts != 2 || oldFailure != 1 || newSuccess != 1 {
		t.Fatalf("history attempts=%d old=%d new=%d error=%v", attempts, oldFailure, newSuccess, e)
	}
}
