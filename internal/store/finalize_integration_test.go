//go:build integration

package store

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
)

func finalFixture(t *testing.T, ctx context.Context) (*pgxpool.Pool, *ClaimRepository, *FinalizationRepository, delivery.ClaimedDelivery) {
	t.Helper()
	pool, claims, _ := claimFixture(t, ctx, 1)
	r, err := NewFinalizationRepository(pool, 3*time.Second, delivery.Policy{Jitter: func(lo, hi time.Duration) time.Duration { return lo }})
	if err != nil {
		t.Fatal(err)
	}
	return pool, claims, r, mustClaim(t, ctx, claims, 1)[0]
}
func sendStatus(code int) delivery.SendResult {
	outcome := delivery.HTTPFailure
	if code >= 200 && code <= 299 {
		outcome = delivery.Acknowledged
	}
	return delivery.SendResult{Outcome: outcome, HTTPStatus: &code, Duration: 17 * time.Millisecond}
}
func finalSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var value string
	if err := pool.QueryRow(ctx, `SELECT json_build_object('delivery',row_to_json(d),'attempts',(SELECT json_agg(a ORDER BY a.attempt_no) FROM attempts a WHERE a.delivery_id=d.id))::text FROM deliveries d WHERE d.id=$1`, id).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}
func TestFinalizationPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	t.Run("acknowledged succeeds once and concurrent duplicate is stale", func(t *testing.T) {
		pool, _, r, c := finalFixture(t, ctx)
		type result struct {
			d   delivery.FinalizationResult
			err error
		}
		done := make(chan result, 2)
		gate := make(chan struct{})
		for i := 0; i < 2; i++ {
			go func() { <-gate; d, e := r.Finalize(ctx, c, sendStatus(204)); done <- result{d, e} }()
		}
		close(gate)
		successes, stales := 0, 0
		for i := 0; i < 2; i++ {
			res := <-done
			if res.err == nil && res.d.Status == "succeeded" {
				successes++
			} else if res.err == ErrStaleClaim {
				stales++
			} else {
				t.Fatal(res)
			}
		}
		if successes != 1 || stales != 1 {
			t.Fatal("duplicate finalization")
		}
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM deliveries d JOIN attempts a ON a.delivery_id=d.id WHERE d.id=$1 AND d.status='succeeded' AND d.confirmed_at=d.updated_at AND d.claim_token IS NULL AND d.lease_until IS NULL AND a.outcome='acknowledged' AND a.http_status=204 AND a.finished_at=d.confirmed_at AND a.duration_ms=17 AND a.error_code IS NULL AND a.retry_delay_ms IS NULL`, c.DeliveryID).Scan(&count); err != nil || count != 1 {
			t.Fatal("inconsistent success", err)
		}
		before := finalSnapshot(t, ctx, pool, c.DeliveryID)
		if _, err := r.Finalize(ctx, c, sendStatus(503)); err != ErrStaleClaim {
			t.Fatal(err)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("duplicate mutated success")
		}
	})
	t.Run("all fence fields and attempt invariant preserve state", func(t *testing.T) {
		pool, _, r, c := finalFixture(t, ctx)
		for _, kind := range []string{"generation", "token", "status", "expired"} {
			bad := c
			switch kind {
			case "generation":
				bad.Generation++
			case "token":
				bad.ClaimToken = "00000000-0000-4000-8000-000000000999"
			case "status":
				claimExec(t, ctx, pool, `UPDATE deliveries SET status='pending',claim_token=NULL,lease_until=NULL WHERE id=$1`, c.DeliveryID)
			case "expired":
				claimExec(t, ctx, pool, `UPDATE deliveries SET status='in_flight',claim_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, c.DeliveryID, c.ClaimToken)
			}
			before := finalSnapshot(t, ctx, pool, c.DeliveryID)
			if _, err := r.Finalize(ctx, bad, sendStatus(200)); err != ErrStaleClaim {
				t.Fatal(kind, err)
			}
			if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
				t.Fatal("stale changed state", kind)
			}
		}
		claimExec(t, ctx, pool, `UPDATE deliveries SET lease_until=clock_timestamp()+interval '1 minute' WHERE id=$1`, c.DeliveryID)
		bad := c
		bad.AttemptID = "00000000-0000-4000-8000-000000000999"
		before := finalSnapshot(t, ctx, pool, c.DeliveryID)
		if _, err := r.Finalize(ctx, bad, sendStatus(200)); err != ErrAttemptInvariant {
			t.Fatal("missing attempt", err)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("attempt mismatch changed state")
		}
		claimExec(t, ctx, pool, `UPDATE attempts SET outcome='unconfirmed',finished_at=clock_timestamp() WHERE id=$1`, c.AttemptID)
		before = finalSnapshot(t, ctx, pool, c.DeliveryID)
		if _, err := r.Finalize(ctx, c, sendStatus(200)); err != ErrAttemptInvariant {
			t.Fatal("finished attempt accepted", err)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("overwrote completed attempt")
		}
	})
	t.Run("retry due time budget exhaustion and historical uncertainty", func(t *testing.T) {
		pool, claims, r, c := finalFixture(t, ctx)
		for n := 1; n <= 5; n++ {
			result := delivery.SendResult{Outcome: delivery.Unconfirmed, ErrorCode: "timeout", Duration: 5 * time.Second}
			d, err := r.Finalize(ctx, c, result)
			if err != nil {
				t.Fatal(err)
			}
			if n == 5 {
				if d.Status != "dead" || d.TerminalReason != "attempts_exhausted" || d.NextAttemptAt != nil {
					t.Fatal(d)
				}
				break
			}
			want := (time.Second << uint(n-1)) / 2
			if d.Status != "pending" || d.RetryDelay != want {
				t.Fatal(n, d)
			}
			var delayMS int64
			var actual time.Duration
			var finished, next time.Time
			if err = pool.QueryRow(ctx, `SELECT a.retry_delay_ms,a.finished_at,d.next_attempt_at FROM attempts a JOIN deliveries d ON d.id=a.delivery_id WHERE a.id=$1 AND a.outcome='unconfirmed' AND a.error_code='timeout' AND d.claim_token IS NULL AND d.lease_until IS NULL`, c.AttemptID).Scan(&delayMS, &finished, &next); err != nil {
				t.Fatal(err)
			}
			actual = next.Sub(finished)
			if actual != want || delayMS != want.Milliseconds() || !next.Equal(*d.NextAttemptAt) {
				t.Fatal("wrong persisted schedule", actual, delayMS)
			}
			if len(mustClaim(t, ctx, claims, 1)) != 0 {
				t.Fatal("premature retry")
			}
			// Observe the real DB due-time boundary instead of a fixed sleep.
			awaitFinalizeDue(t, ctx, pool, c.DeliveryID)
			c = mustClaim(t, ctx, claims, 1)[0]
		}
		if len(mustClaim(t, ctx, claims, 1)) != 0 {
			t.Fatal("sixth claim eligible")
		}
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE delivery_id=$1 AND outcome='unconfirmed' AND error_code='timeout' AND finished_at IS NOT NULL`, c.DeliveryID).Scan(&count); err != nil || count != 5 {
			t.Fatal("history lost", err, count)
		}
	})
	t.Run("permanent HTTP local failure and Retry-After scheduling", func(t *testing.T) {
		for _, code := range []int{301, 400, 401, 403, 404, 408, 429, 503} {
			t.Run(fmt.Sprint(code), func(t *testing.T) {
				pool, _, r, c := finalFixture(t, ctx)
				result := sendStatus(code)
				result.RetryAfter = []string{"30"}
				d, err := r.Finalize(ctx, c, result)
				if err != nil {
					t.Fatal(err)
				}
				if code == 429 || code == 503 {
					if d.Status != "pending" || d.RetryDelay != 30*time.Second {
						t.Fatal(d)
					}
				} else if d.Status != "dead" || d.TerminalReason != "permanent_http" {
					t.Fatal(d)
				}
				var count int
				if err = pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE id=$1 AND claim_token IS NULL AND lease_until IS NULL`, c.DeliveryID).Scan(&count); err != nil || count != 1 {
					t.Fatal("lease not cleared")
				}
			})
		}
		pool, _, r, c := finalFixture(t, ctx)
		d, err := r.Finalize(ctx, c, delivery.SendResult{Outcome: delivery.LocalFailure, ErrorCode: "missing_signing_key"})
		if err != nil || d.TerminalReason != "local_configuration" {
			t.Fatal(d, err)
		}
		var count int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE id=$1 AND outcome='local_failure' AND error_code='missing_signing_key' AND retry_delay_ms IS NULL`, c.AttemptID).Scan(&count); err != nil || count != 1 {
			t.Fatal("local failure not retained")
		}
	})
	t.Run("later acknowledgment preserves earlier uncertainty", func(t *testing.T) {
		pool, claims, r, first := finalFixture(t, ctx)
		if _, err := r.Finalize(ctx, first, delivery.SendResult{Outcome: delivery.Unconfirmed, ErrorCode: "network_error"}); err != nil {
			t.Fatal(err)
		}
		awaitFinalizeDue(t, ctx, pool, first.DeliveryID)
		second := mustClaim(t, ctx, claims, 1)[0]
		before := finalSnapshot(t, ctx, pool, first.DeliveryID)
		if _, err := r.Finalize(ctx, first, sendStatus(204)); err != ErrStaleClaim {
			t.Fatal(err)
		}
		if before != finalSnapshot(t, ctx, pool, first.DeliveryID) {
			t.Fatal("old generation mutated new attempt")
		}
		if _, err := r.Finalize(ctx, second, sendStatus(204)); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE delivery_id=$1 AND outcome='unconfirmed' AND error_code='network_error'`, first.DeliveryID).Scan(&count); err != nil || count != 1 {
			t.Fatal("later success erased uncertainty")
		}
	})
	t.Run("HTTP-date schedule uses database finalization time", func(t *testing.T) {
		pool, _, r, c := finalFixture(t, ctx)
		var now time.Time
		if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			t.Fatal(err)
		}
		date := now.UTC().Add(20 * time.Second).Truncate(time.Second)
		result := sendStatus(503)
		result.RetryAfter = []string{date.Format(http.TimeFormat)}
		d, err := r.Finalize(ctx, c, result)
		if err != nil || d.NextAttemptAt == nil || !d.NextAttemptAt.Equal(date) {
			t.Fatal("date schedule differs", d, err)
		}
		var next, finished time.Time
		var delay int64
		if err = pool.QueryRow(ctx, `SELECT d.next_attempt_at,a.finished_at,a.retry_delay_ms FROM deliveries d JOIN attempts a ON a.delivery_id=d.id WHERE a.id=$1`, c.AttemptID).Scan(&next, &finished, &delay); err != nil {
			t.Fatal(err)
		}
		if !next.Equal(date) || next.Sub(finished).Milliseconds() != delay {
			t.Fatal("date delay not persisted from DB timestamp")
		}
	})
	t.Run("rollback after attempt update and at commit", func(t *testing.T) {
		pool, _, r, c := finalFixture(t, ctx)
		claimExec(t, ctx, pool, `ALTER TABLE deliveries ADD CONSTRAINT test_fail_finalize CHECK(status <> 'succeeded')`)
		before := finalSnapshot(t, ctx, pool, c.DeliveryID)
		if _, err := r.Finalize(ctx, c, sendStatus(204)); err != ErrFinalizeUnavailable {
			t.Fatal(err)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("partial attempt update committed")
		}
		claimExec(t, ctx, pool, `ALTER TABLE deliveries DROP CONSTRAINT test_fail_finalize`)
		claimExec(t, ctx, pool, `CREATE FUNCTION test_finalize_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test commit failure'; END $$; CREATE CONSTRAINT TRIGGER test_finalize_commit_failure AFTER UPDATE ON deliveries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION test_finalize_commit_failure()`)
		before = finalSnapshot(t, ctx, pool, c.DeliveryID)
		if _, err := r.Finalize(ctx, c, sendStatus(204)); err != ErrFinalizeCommitUnknown {
			t.Fatal(err)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("commit failure changed state")
		}
		claimExec(t, ctx, pool, `DROP TRIGGER test_finalize_commit_failure ON deliveries; DROP FUNCTION test_finalize_commit_failure()`)
		if d, err := r.Finalize(ctx, c, sendStatus(204)); err != nil || d.Status != "succeeded" {
			t.Fatal("rollback made claim unusable", err)
		}
	})
	t.Run("expired after row lock wait rejects using fresh SQL clock", func(t *testing.T) {
		pool, _, r, c := finalFixture(t, ctx)
		blocker, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(ctx)
		if _, err = blocker.Exec(ctx, `SELECT id FROM deliveries WHERE id=$1 FOR UPDATE`, c.DeliveryID); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { _, err := r.Finalize(ctx, c, sendStatus(204)); done <- err }()
		awaitClaimState(t, ctx, pool, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT status,generation,claim_token%')`)
		if _, err = blocker.Exec(ctx, `UPDATE deliveries SET lease_until=clock_timestamp() WHERE id=$1`, c.DeliveryID); err != nil {
			t.Fatal(err)
		}
		if err = blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		before := finalSnapshot(t, ctx, pool, c.DeliveryID)
		if err = <-done; err != ErrStaleClaim {
			t.Fatal("accepted expired lease after waiting", err)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("expired claim mutated state")
		}
	})
}
func awaitFinalizeDue(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var due bool
		if err := pool.QueryRow(ctx, `SELECT next_attempt_at<=clock_timestamp() FROM deliveries WHERE id=$1`, id).Scan(&due); err != nil {
			t.Fatal(err)
		}
		if due {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("retry did not become due")
		}
	}
}
