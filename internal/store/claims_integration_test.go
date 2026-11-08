//go:build integration

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func claimFixture(t *testing.T, ctx context.Context, n int) (*pgxpool.Pool, *ClaimRepository, []string) {
	t.Helper()
	db, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	applyEventSchema(t, ctx, db.Pool)
	registry, err := config.RegistryForMode(config.RunModeCompose)
	if err != nil {
		t.Fatal(err)
	}
	if err = Seed(ctx, db.Pool, registry); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		req, err := ValidateEventRequest(eventRequestBody(registry.Destinations()[0].ID))
		if err != nil {
			t.Fatal(err)
		}
		accepted, err := NewEventRepository(db.Pool).AcceptEvent(ctx, registry.Tenant().ID, fmt.Sprintf("claim-%d", i), req)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, accepted.DeliveryIDs[0])
	}
	if _, err = db.Pool.Exec(ctx, `UPDATE deliveries SET next_attempt_at='2000-01-01T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}
	sort.Strings(ids)
	r, err := NewClaimRepository(db.Pool, 30*time.Second, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return db.Pool, r, ids
}
func mustClaim(t *testing.T, ctx context.Context, r *ClaimRepository, slots int) []delivery.ClaimedDelivery {
	t.Helper()
	out, err := r.ClaimDue(ctx, slots)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func claimExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
}
func assertClaimsUntouched(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int) {
	t.Helper()
	var pristine, attempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE status='pending' AND generation=0 AND cycle_attempt_count=0 AND lifetime_attempt_count=0 AND claim_token IS NULL AND lease_until IS NULL`).Scan(&pristine); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if pristine != n || attempts != 0 {
		t.Fatalf("rollback: pristine=%d attempts=%d want %d/0", pristine, attempts, n)
	}
}

// Poll a database-observable barrier with a deadline; never assume a sleep
// allowed another goroutine to reach a particular lock or commit boundary.
func awaitClaimState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		var ready bool
		if err := pool.QueryRow(ctx, sql).Scan(&ready); err != nil {
			t.Fatal("observe database barrier", err)
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("database barrier deadline")
		case <-tick.C:
		}
	}
}

func TestClaimDuePostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	t.Run("concurrent claimers bounded and unique", func(t *testing.T) {
		pool, r, ids := claimFixture(t, ctx, 10)
		type result struct {
			claims []delivery.ClaimedDelivery
			err    error
		}
		results := make(chan result, 2)
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			go func() { <-start; c, e := r.ClaimDue(ctx, 3); results <- result{c, e} }()
		}
		close(start)
		seen := map[string]bool{}
		tokens := map[string]bool{}
		attemptIDs := map[string]bool{}
		for i := 0; i < 2; i++ {
			res := <-results
			if res.err != nil {
				t.Fatal(res.err)
			}
			if len(res.claims) != 3 {
				t.Fatalf("claim count %d want 3", len(res.claims))
			}
			for _, c := range res.claims {
				if seen[c.DeliveryID] || tokens[c.ClaimToken] || attemptIDs[c.AttemptID] {
					t.Fatal("shared claim or fence")
				}
				seen[c.DeliveryID] = true
				tokens[c.ClaimToken] = true
				attemptIDs[c.AttemptID] = true
				if c.Generation != 1 || c.CycleNo != 1 || c.CycleAttemptNo != 1 || c.AttemptNo != 1 || !c.LeaseUntil.Equal(c.StartedAt.Add(30*time.Second)) {
					t.Fatalf("invalid claim %+v", c)
				}
				var count int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM attempts a JOIN deliveries d ON d.id=a.delivery_id WHERE a.id=$1 AND a.outcome='running' AND a.finished_at IS NULL AND a.generation=d.generation AND a.claim_token=d.claim_token AND a.attempt_no=d.lifetime_attempt_count AND a.cycle_attempt_no=d.cycle_attempt_count AND a.started_at=d.updated_at AND a.claim_token=$2`, c.AttemptID, c.ClaimToken).Scan(&count); err != nil || count != 1 {
					t.Fatal("attempt not persisted atomically", err)
				}
			}
		}
		remaining := mustClaim(t, ctx, r, MaxClaimSlots)
		if len(remaining) != len(ids)-len(seen) {
			t.Fatal("wrong remaining claim count")
		}
		for _, c := range remaining {
			if seen[c.DeliveryID] {
				t.Fatal("live claim returned twice")
			}
		}
		if len(mustClaim(t, ctx, r, 1)) != 0 {
			t.Fatal("in-flight delivery reclaimed")
		}
	})
	t.Run("selection excludes future terminal inflight exhausted", func(t *testing.T) {
		pool, r, ids := claimFixture(t, ctx, 6)
		claimExec(t, ctx, pool, `UPDATE deliveries SET next_attempt_at=clock_timestamp()+interval '1 day' WHERE id=$1`, ids[0])
		claimExec(t, ctx, pool, `UPDATE deliveries SET status='succeeded',confirmed_at=clock_timestamp() WHERE id=$1`, ids[1])
		claimExec(t, ctx, pool, `UPDATE deliveries SET status='dead',terminal_reason='permanent_http' WHERE id=$1`, ids[2])
		claimExec(t, ctx, pool, `UPDATE deliveries SET status='in_flight',claim_token=$2,lease_until=clock_timestamp()-interval '1 hour' WHERE id=$1`, ids[3], ids[0])
		claimExec(t, ctx, pool, `UPDATE deliveries SET cycle_attempt_count=5,lifetime_attempt_count=5 WHERE id=$1`, ids[4])
		claims := mustClaim(t, ctx, r, 32)
		if len(claims) != 1 || claims[0].DeliveryID != ids[5] {
			t.Fatalf("claimed excluded row: %+v", claims)
		}
	})
	t.Run("skip locked and due ordering", func(t *testing.T) {
		pool, r, ids := claimFixture(t, ctx, 3)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, `SELECT id FROM deliveries WHERE id=$1 FOR UPDATE`, ids[0]); err != nil {
			t.Fatal(err)
		}
		c := mustClaim(t, ctx, r, 1)
		if len(c) != 1 || c[0].DeliveryID != ids[1] {
			t.Fatal("did not skip locked oldest row")
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		c = mustClaim(t, ctx, r, 1)
		if len(c) != 1 || c[0].DeliveryID != ids[0] {
			t.Fatal("due tie-break order changed")
		}
	})
	t.Run("attempt insert failure rolls back entire batch", func(t *testing.T) {
		pool, r, ids := claimFixture(t, ctx, 2)
		// Fail the second ordered insert so the first attempt must also roll back.
		claimExec(t, ctx, pool, fmt.Sprintf(`ALTER TABLE attempts ADD CONSTRAINT test_reject_claim CHECK (delivery_id <> '%s'::uuid)`, ids[1]))
		c, err := r.ClaimDue(ctx, 2)
		if err != ErrClaimUnavailable || c != nil {
			t.Fatal("failed batch exposed claims", err)
		}
		assertClaimsUntouched(t, ctx, pool, 2)
		claimExec(t, ctx, pool, `ALTER TABLE attempts DROP CONSTRAINT test_reject_claim`)
		if len(mustClaim(t, ctx, r, 2)) != 2 {
			t.Fatal("rollback left rows unclaimable")
		}
	})
	t.Run("commit failure returns no send data", func(t *testing.T) {
		pool, r, _ := claimFixture(t, ctx, 2)
		claimExec(t, ctx, pool, `CREATE FUNCTION test_claim_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test commit failure'; END $$; CREATE CONSTRAINT TRIGGER test_claim_commit_failure AFTER INSERT ON attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION test_claim_commit_failure()`)
		c, err := r.ClaimDue(ctx, 2)
		if err != ErrClaimCommitUnknown || c != nil {
			t.Fatal("unconfirmed commit exposed claim", err)
		}
		assertClaimsUntouched(t, ctx, pool, 2)
	})
	t.Run("exact immutable send data and released connection locks", func(t *testing.T) {
		pool, r, ids := claimFixture(t, ctx, 1)
		raw := []byte(" {\"data\":\"नमस्ते 🌍\"}\n")
		claimExec(t, ctx, pool, `UPDATE events SET wire_body=$1`, raw)
		// Locking events must not block claim mutation/commit. It should only block
		// the subsequent immutable data load, after the delivery lock is released.
		blocker, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(ctx)
		if _, err = blocker.Exec(ctx, `LOCK TABLE events IN ACCESS EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
		type result struct {
			c   []delivery.ClaimedDelivery
			err error
		}
		done := make(chan result, 1)
		go func() { c, e := r.ClaimDue(ctx, 1); done <- result{c, e} }()
		awaitClaimState(t, ctx, pool, `SELECT EXISTS(SELECT 1 FROM deliveries WHERE status='in_flight')`)
		// Another transaction can lock the delivery while send-data loading waits.
		probe, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer probe.Rollback(ctx)
		if _, err = probe.Exec(ctx, `SELECT id FROM deliveries WHERE id=$1 FOR UPDATE NOWAIT`, ids[0]); err != nil {
			t.Fatal("claim retains delivery lock", err)
		}
		if err = probe.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		res := <-done
		if res.err != nil || len(res.c) != 1 {
			t.Fatal("load data", res.err)
		}
		c := res.c[0]
		if !bytes.Equal(c.WireBody, raw) || c.URLSnapshot != "http://fulfillment:8081/webhook" || c.KeyRefSnapshot != "fulfillment-v1" {
			t.Fatal("send data changed")
		}
		c.WireBody[0] = 'x'
		var stored []byte
		if err = pool.QueryRow(ctx, `SELECT wire_body FROM events WHERE id=$1`, c.EventID).Scan(&stored); err != nil || !bytes.Equal(stored, raw) {
			t.Fatal("returned data aliases persistent data")
		}
		if pool.Stat().AcquiredConns() != 0 {
			t.Fatal("claim retained pool connection")
		}
	})
	t.Run("send-data load timeout exposes no claims after commit", func(t *testing.T) {
		pool, _, _ := claimFixture(t, ctx, 1)
		r, err := NewClaimRepository(pool, 30*time.Second, 200*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		blocker, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(ctx)
		if _, err = blocker.Exec(ctx, `LOCK TABLE events IN ACCESS EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
		c, err := r.ClaimDue(ctx, 1)
		if c != nil || err != ErrClaimUnavailable {
			t.Fatal("failed immutable load exposed send data", err)
		}
		if err = blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var attempts int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM deliveries d JOIN attempts a ON a.delivery_id=d.id WHERE d.status='in_flight' AND a.outcome='running' AND d.generation=1`).Scan(&attempts); err != nil || attempts != 1 {
			t.Fatal("committed claim not left for recovery", err)
		}
		if len(mustClaim(t, ctx, r, 1)) != 0 {
			t.Fatal("reissued committed claim after failed data load")
		}
	})
	t.Run("SQL clock captured after lock wait", func(t *testing.T) {
		pool, r, _ := claimFixture(t, ctx, 1)
		blocker, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(ctx)
		if _, err = blocker.Exec(ctx, `LOCK TABLE deliveries IN ACCESS EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
		type result struct {
			c   []delivery.ClaimedDelivery
			err error
		}
		done := make(chan result, 1)
		go func() { c, e := r.ClaimDue(ctx, 1); done <- result{c, e} }()
		awaitClaimState(t, ctx, pool, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT id::text FROM deliveries%')`)
		var releasedAfter time.Time
		if err = blocker.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&releasedAfter); err != nil {
			t.Fatal(err)
		}
		if err = blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		res := <-done
		if res.err != nil || len(res.c) != 1 {
			t.Fatal("claim after lock wait", res.err)
		}
		if res.c[0].StartedAt.Before(releasedAfter) {
			t.Fatal("used transaction-start clock before locks")
		}
	})
	t.Run("bounded lock wait and cancellation", func(t *testing.T) {
		pool, _, _ := claimFixture(t, ctx, 1)
		r, err := NewClaimRepository(pool, time.Second, 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		blocker, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(ctx)
		if _, err = blocker.Exec(ctx, `LOCK TABLE deliveries IN ACCESS EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
		deadline, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		c, err := r.ClaimDue(deadline, 1)
		if c != nil || !errors.Is(err, ErrClaimUnavailable) || deadline.Err() != nil {
			t.Fatal("repository did not bound lock wait", err)
		}
		if err = blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		assertClaimsUntouched(t, ctx, pool, 1)
	})
	t.Run("fresh claim fences lifetime numbering across cycles", func(t *testing.T) {
		pool, r, ids := claimFixture(t, ctx, 1)
		first := mustClaim(t, ctx, r, 1)[0]
		// Test-only state transition stands in for later finalize/replay tasks.
		claimExec(t, ctx, pool, `UPDATE attempts SET outcome='unconfirmed',finished_at=clock_timestamp() WHERE delivery_id=$1`, ids[0])
		claimExec(t, ctx, pool, `UPDATE deliveries SET status='pending',claim_token=NULL,lease_until=NULL WHERE id=$1`, ids[0])
		second := mustClaim(t, ctx, r, 1)[0]
		claimExec(t, ctx, pool, `UPDATE attempts SET outcome='unconfirmed',finished_at=clock_timestamp() WHERE delivery_id=$1 AND outcome='running'`, ids[0])
		claimExec(t, ctx, pool, `UPDATE deliveries SET status='pending',claim_token=NULL,lease_until=NULL,cycle_no=cycle_no+1,cycle_attempt_count=0 WHERE id=$1`, ids[0])
		third := mustClaim(t, ctx, r, 1)[0]
		if first.ClaimToken == second.ClaimToken || second.ClaimToken == third.ClaimToken || first.AttemptID == second.AttemptID || second.Generation != 2 || third.Generation != 3 || second.AttemptNo != 2 || third.AttemptNo != 3 || third.CycleNo != 2 || third.CycleAttemptNo != 1 {
			t.Fatal("claim fence or numbering reused")
		}
		if first.DeliveryID != third.DeliveryID || !bytes.Equal(first.WireBody, third.WireBody) {
			t.Fatal("operation/body changed across cycles")
		}
	})
}
