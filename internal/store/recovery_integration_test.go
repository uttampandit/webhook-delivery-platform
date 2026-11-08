//go:build integration

package store

import (
	"context"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"testing"
	"time"
)

func TestRecoveryPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	t.Run("five expiries retain history and consume budget", func(t *testing.T) {
		pool, claims, finalizer, c := finalFixture(t, ctx)
		r, _ := NewRecoveryRepository(pool, 3*time.Second, delivery.Policy{Jitter: func(lo, hi time.Duration) time.Duration { return lo }})
		for i := 1; i <= 5; i++ {
			claimExec(t, ctx, pool, `UPDATE deliveries SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, c.DeliveryID)
			n, err := r.RecoverExpired(ctx)
			if err != nil || n != 1 {
				t.Fatal(n, err)
			}
			var valid bool
			err = pool.QueryRow(ctx, `SELECT d.generation=$2 AND d.cycle_attempt_count=$3 AND d.lifetime_attempt_count=$3 AND d.claim_token IS NULL AND d.lease_until IS NULL AND a.outcome='unconfirmed' AND a.error_code='lease_expired' AND a.finished_at=d.updated_at AND a.duration_ms=GREATEST(0,trunc(extract(epoch FROM (a.finished_at-a.started_at))*1000)) AND a.http_status IS NULL FROM deliveries d JOIN attempts a ON a.delivery_id=d.id WHERE a.id=$1`, c.AttemptID, c.Generation+1, i).Scan(&valid)
			if err != nil || !valid {
				t.Fatal("history/fence", valid, err)
			}
			before := finalSnapshot(t, ctx, pool, c.DeliveryID)
			if _, err = finalizer.Finalize(ctx, c, sendStatus(204)); err != ErrStaleClaim {
				t.Fatal(err)
			}
			if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
				t.Fatal("stale mutation")
			}
			if i < 5 {
				var delay int64
				var next, finished time.Time
				if err = pool.QueryRow(ctx, `SELECT a.retry_delay_ms,d.next_attempt_at,a.finished_at FROM attempts a JOIN deliveries d ON d.id=a.delivery_id WHERE a.id=$1 AND d.status='pending'`, c.AttemptID).Scan(&delay, &next, &finished); err != nil {
					t.Fatal(err)
				}
				want := (time.Second << uint(i-1)) / 2
				if delay != want.Milliseconds() || next.Sub(finished) != want {
					t.Fatal("backoff", delay, next.Sub(finished))
				}
				if len(mustClaim(t, ctx, claims, 1)) != 0 {
					t.Fatal("early retry")
				}
				claimExec(t, ctx, pool, `UPDATE deliveries SET next_attempt_at=clock_timestamp() WHERE id=$1`, c.DeliveryID)
				c = mustClaim(t, ctx, claims, 1)[0]
			}
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM deliveries d JOIN attempts a ON a.delivery_id=d.id WHERE d.id=$1 AND d.status='dead' AND d.terminal_reason='attempts_exhausted' AND a.outcome='unconfirmed'`, c.DeliveryID).Scan(&n); err != nil || n != 5 {
			t.Fatal(n, err)
		}
		if len(mustClaim(t, ctx, claims, 1)) != 0 {
			t.Fatal("sixth claim")
		}
	})
	t.Run("concurrent reapers and expired finalizer converge", func(t *testing.T) {
		pool, _, f, c := finalFixture(t, ctx)
		r, _ := NewRecoveryRepository(pool, 3*time.Second, delivery.Policy{})
		claimExec(t, ctx, pool, `UPDATE deliveries SET lease_until=clock_timestamp() WHERE id=$1`, c.DeliveryID)
		gate := make(chan struct{})
		recovered := make(chan int, 2)
		failures := make(chan error, 3)
		for i := 0; i < 2; i++ {
			go func() { <-gate; n, e := r.RecoverExpired(ctx); recovered <- n; failures <- e }()
		}
		go func() {
			<-gate
			_, e := f.Finalize(ctx, c, sendStatus(204))
			if e == ErrStaleClaim {
				e = nil
			}
			failures <- e
		}()
		close(gate)
		for i := 0; i < 3; i++ {
			if e := <-failures; e != nil {
				t.Fatal(e)
			}
		}
		if n := <-recovered + <-recovered; n != 1 {
			t.Fatal("recovered twice", n)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE delivery_id=$1 AND outcome='unconfirmed' AND error_code='lease_expired'`, c.DeliveryID).Scan(&n); err != nil || n != 1 {
			t.Fatal(n, err)
		}
	})
	t.Run("unexpired finalization wins and recovery preserves success", func(t *testing.T) {
		pool, _, f, c := finalFixture(t, ctx)
		r, _ := NewRecoveryRepository(pool, 3*time.Second, delivery.Policy{})
		done := make(chan error, 2)
		go func() { _, e := f.Finalize(ctx, c, sendStatus(204)); done <- e }()
		go func() { _, e := r.RecoverExpired(ctx); done <- e }()
		for i := 0; i < 2; i++ {
			if e := <-done; e != nil {
				t.Fatal(e)
			}
		}
		before := finalSnapshot(t, ctx, pool, c.DeliveryID)
		if n, e := r.RecoverExpired(ctx); n != 0 || e != nil {
			t.Fatal(n, e)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("success overwritten")
		}
	})
	t.Run("inconsistent attempt rolls back entire batch", func(t *testing.T) {
		pool, claims, ids := claimFixture(t, ctx, 2)
		cs := mustClaim(t, ctx, claims, 2)
		r, _ := NewRecoveryRepository(pool, 3*time.Second, delivery.Policy{})
		claimExec(t, ctx, pool, `UPDATE deliveries SET lease_until=clock_timestamp()-interval '1 second'`)
		claimExec(t, ctx, pool, `UPDATE attempts SET outcome='unconfirmed',finished_at=clock_timestamp() WHERE id=$1`, cs[1].AttemptID)
		before := []string{finalSnapshot(t, ctx, pool, ids[0]), finalSnapshot(t, ctx, pool, ids[1])}
		if n, e := r.RecoverExpired(ctx); n != 0 || e != ErrAttemptInvariant {
			t.Fatal(n, e)
		}
		for i, id := range ids {
			if before[i] != finalSnapshot(t, ctx, pool, id) {
				t.Fatal("partial repair")
			}
		}
	})
	t.Run("write and commit failures roll back history", func(t *testing.T) {
		pool, _, _, c := finalFixture(t, ctx)
		r, _ := NewRecoveryRepository(pool, 3*time.Second, delivery.Policy{})
		claimExec(t, ctx, pool, `UPDATE deliveries SET lease_until=clock_timestamp() WHERE id=$1`, c.DeliveryID)
		claimExec(t, ctx, pool, `ALTER TABLE deliveries ADD CONSTRAINT test_recovery_failure CHECK(status<>'pending')`)
		before := finalSnapshot(t, ctx, pool, c.DeliveryID)
		if n, e := r.RecoverExpired(ctx); n != 0 || e != ErrRecoveryUnavailable {
			t.Fatal(n, e)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("partial attempt update")
		}
		claimExec(t, ctx, pool, `ALTER TABLE deliveries DROP CONSTRAINT test_recovery_failure`)
		claimExec(t, ctx, pool, `CREATE FUNCTION test_recovery_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test commit failure'; END $$; CREATE CONSTRAINT TRIGGER test_recovery_commit_failure AFTER UPDATE ON deliveries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION test_recovery_commit_failure()`)
		if n, e := r.RecoverExpired(ctx); n != 0 || e != ErrRecoveryCommitUnknown {
			t.Fatal(n, e)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("commit failure changed history")
		}
	})

	t.Run("bounded hundred and skip locked", func(t *testing.T) {
		pool, claims, _ := claimFixture(t, ctx, 102)
		for i := 0; i < 4; i++ {
			mustClaim(t, ctx, claims, 32)
		}
		claimExec(t, ctx, pool, `UPDATE deliveries SET lease_until=clock_timestamp()-interval '1 second'`)
		blocker, e := pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer blocker.Rollback(ctx)
		var locked string
		if e = blocker.QueryRow(ctx, `SELECT id::text FROM deliveries ORDER BY id LIMIT 1 FOR UPDATE`).Scan(&locked); e != nil {
			t.Fatal(e)
		}
		r, _ := NewRecoveryRepository(pool, 3*time.Second, delivery.Policy{})
		if n, e := r.RecoverExpired(ctx); n != 100 || e != nil {
			t.Fatal(n, e)
		}
		if n, e := r.RecoverExpired(ctx); n != 1 || e != nil {
			t.Fatal(n, e)
		}
		if e = blocker.Commit(ctx); e != nil {
			t.Fatal(e)
		}
		if n, e := r.RecoverExpired(ctx); n != 1 || e != nil {
			t.Fatal(n, e)
		}
	})
}
