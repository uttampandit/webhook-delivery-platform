//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
)

func replayFixture(t *testing.T, ctx context.Context, n int) (*pgxpool.Pool, *ReplayRepository, []string) {
	t.Helper()
	pool, _, ids := claimFixture(t, ctx, n)
	claimExec(t, ctx, pool, `UPDATE deliveries SET status='dead',terminal_reason='permanent_http'`)
	return pool, NewReplayRepository(pool), ids
}
func replayAuditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int) {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM replays`).Scan(&n); err != nil || n != want {
		t.Fatal("audit count", n, err)
	}
}
func TestReplayRaceMatrixPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, kind := range []string{"same key same target", "different keys same target", "same key different targets"} {
		t.Run(kind, func(t *testing.T) {
			pool, r, ids := replayFixture(t, ctx, 2)
			before := map[string]string{}
			for _, id := range ids {
				before[id] = finalSnapshot(t, ctx, pool, id)
			}
			blocker, e := pool.Begin(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer blocker.Rollback(ctx)
			if _, e = blocker.Exec(ctx, `SELECT id FROM deliveries ORDER BY id FOR UPDATE`); e != nil {
				t.Fatal(e)
			}
			type result struct {
				value ReplayResult
				err   error
				id    string
			}
			done := make(chan result, 2)
			for i := 0; i < 2; i++ {
				id, key := ids[0], "replay-key"
				if i == 1 && kind == "different keys same target" {
					key = "other-key"
				}
				if i == 1 && kind == "same key different targets" {
					id = ids[1]
				}
				go func(id, key string) {
					v, e := r.Replay(ctx, config.TenantID, id, key, "local-operator")
					done <- result{v, e, id}
				}(id, key)
			}
			// Both transactions have observed absent keys and reached row-lock waits.
			awaitClaimState(t, ctx, pool, `SELECT count(*)>=2 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT status,cycle_no FROM deliveries%'`)
			if e = blocker.Commit(ctx); e != nil {
				t.Fatal(e)
			}
			a, b := <-done, <-done
			replayAuditCount(t, ctx, pool, 1)
			if kind == "same key same target" {
				if a.err != nil || b.err != nil || a.value.ReplayID != b.value.ReplayID || a.value.CycleNo != 2 || b.value.CycleNo != 2 || a.value.Deduplicated == b.value.Deduplicated {
					t.Fatal(a, b)
				}
			} else {
				if a.err != nil {
					a, b = b, a
				}
				want := ErrInvalidState
				if kind == "same key different targets" {
					want = ErrIdempotencyConflict
				}
				if a.err != nil || b.err != want || a.value.Deduplicated {
					t.Fatal(a, b)
				}
				if kind == "same key different targets" && before[b.id] != finalSnapshot(t, ctx, pool, b.id) {
					t.Fatal("loser mutated")
				}
			}
		})
	}
}
func immutableReplaySnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var s string
	err := pool.QueryRow(ctx, `SELECT json_build_array(d.id,d.event_id,d.destination_id,d.url_snapshot,d.key_ref_snapshot,d.lifetime_attempt_count,e.idempotency_key,encode(e.wire_body,'hex'),encode(e.request_body,'hex'),encode(e.request_hash,'hex'))::text FROM deliveries d JOIN events e ON e.id=d.event_id WHERE d.id=$1`, id).Scan(&s)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestReplayCyclesAndStatePostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, claims, finalizer, c := finalFixture(t, ctx)
	// Reach dead through real finalization, retaining five uncertain attempts.
	for i := 1; i <= 5; i++ {
		if _, err := finalizer.Finalize(ctx, c, delivery.SendResult{Outcome: delivery.Unconfirmed, ErrorCode: "timeout"}); err != nil {
			t.Fatal(err)
		}
		if i < 5 {
			claimExec(t, ctx, pool, `UPDATE deliveries SET next_attempt_at=clock_timestamp() WHERE id=$1`, c.DeliveryID)
			c = mustClaim(t, ctx, claims, 1)[0]
		}
	}
	r := NewReplayRepository(pool)
	immutable := immutableReplaySnapshot(t, ctx, pool, c.DeliveryID)
	var history string
	if e := pool.QueryRow(ctx, `SELECT json_agg(a ORDER BY attempt_no)::text FROM attempts a WHERE delivery_id=$1`, c.DeliveryID).Scan(&history); e != nil {
		t.Fatal(e)
	}
	// An UPDATE trigger observes the audit inside the transaction, proving order.
	claimExec(t, ctx, pool, `CREATE FUNCTION test_require_replay_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.status='dead' AND NEW.status='pending' AND NOT EXISTS(SELECT 1 FROM replays WHERE delivery_id=NEW.id AND from_cycle=OLD.cycle_no AND to_cycle=NEW.cycle_no) THEN RAISE EXCEPTION 'audit must precede update'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_require_replay_audit BEFORE UPDATE ON deliveries FOR EACH ROW EXECUTE FUNCTION test_require_replay_audit()`)
	first, e := r.Replay(ctx, config.TenantID, c.DeliveryID, "cycle-two", "local-operator")
	if e != nil || first.CycleNo != 2 || first.Deduplicated {
		t.Fatal(first, e)
	}
	if immutable != immutableReplaySnapshot(t, ctx, pool, c.DeliveryID) {
		t.Fatal("operation changed")
	}
	var valid bool
	if e = pool.QueryRow(ctx, `SELECT status='pending' AND cycle_no=2 AND cycle_attempt_count=0 AND lifetime_attempt_count=5 AND generation=$2 AND terminal_reason IS NULL AND claim_token IS NULL AND lease_until IS NULL AND next_attempt_at=updated_at AND next_attempt_at<=clock_timestamp() FROM deliveries WHERE id=$1`, c.DeliveryID, c.Generation+1).Scan(&valid); e != nil || !valid {
		t.Fatal("reset fields", valid, e)
	}
	var after string
	if e = pool.QueryRow(ctx, `SELECT json_agg(a ORDER BY attempt_no)::text FROM attempts a WHERE delivery_id=$1`, c.DeliveryID).Scan(&after); e != nil || after != history {
		t.Fatal("old attempts changed", e)
	}
	checkDuplicate := func() {
		t.Helper()
		got, e := r.Replay(ctx, config.TenantID, c.DeliveryID, "cycle-two", "local-operator")
		if e != nil || !got.Deduplicated || got.CycleNo != 2 || got.ReplayID != first.ReplayID {
			t.Fatal(got, e)
		}
	}
	rejectNew := func(key string) {
		t.Helper()
		before := finalSnapshot(t, ctx, pool, c.DeliveryID)
		if _, e := r.Replay(ctx, config.TenantID, c.DeliveryID, key, "local-operator"); e != ErrInvalidState {
			t.Fatal(e)
		}
		if before != finalSnapshot(t, ctx, pool, c.DeliveryID) {
			t.Fatal("rejected replay mutated")
		}
	}
	checkDuplicate()
	rejectNew("pending-reject")
	secondClaim := mustClaim(t, ctx, claims, 1)[0]
	if secondClaim.DeliveryID != c.DeliveryID || secondClaim.CycleNo != 2 || secondClaim.AttemptNo != 6 || secondClaim.CycleAttemptNo != 1 || string(secondClaim.WireBody) != string(c.WireBody) || secondClaim.KeyRefSnapshot != c.KeyRefSnapshot || secondClaim.URLSnapshot != c.URLSnapshot {
		t.Fatal("new cycle identity", secondClaim)
	}
	checkDuplicate()
	rejectNew("inflight-reject")
	if _, e = finalizer.Finalize(ctx, secondClaim, sendStatus(400)); e != nil {
		t.Fatal(e)
	}
	checkDuplicate()
	third, e := r.Replay(ctx, config.TenantID, c.DeliveryID, "cycle-three", "local-operator")
	if e != nil || third.CycleNo != 3 {
		t.Fatal(third, e)
	}
	checkDuplicate()
	thirdClaim := mustClaim(t, ctx, claims, 1)[0]
	if thirdClaim.AttemptNo != 7 || thirdClaim.CycleNo != 3 || thirdClaim.CycleAttemptNo != 1 {
		t.Fatal(thirdClaim)
	}
	if _, e = finalizer.Finalize(ctx, thirdClaim, sendStatus(204)); e != nil {
		t.Fatal(e)
	}
	checkDuplicate()
	rejectNew("success-reject")
	replayAuditCount(t, ctx, pool, 2)
	detail, e := NewQueryRepository(pool).GetEvent(ctx, config.TenantID, c.EventID)
	if e != nil || len(detail.Deliveries) != 1 || len(detail.Deliveries[0].Replays) != 2 || len(detail.Deliveries[0].Attempts) != 7 {
		t.Fatal("detail timeline", e)
	}
}
func TestReplayIsolationAndRollbackPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	t.Run("foreign unknown and validation", func(t *testing.T) {
		pool, r, ids := replayFixture(t, ctx, 1)
		before := finalSnapshot(t, ctx, pool, ids[0])
		for _, input := range []struct {
			tenant, id, key, actor string
			want                   error
		}{
			{"00000000-0000-4000-8000-000000000099", ids[0], "k", "local-operator", ErrNotFound},
			{config.TenantID, "00000000-0000-4000-8000-000000000099", "k", "local-operator", ErrNotFound},
			{config.TenantID, "bad", "k", "local-operator", ErrInvalidInput},
			{config.TenantID, ids[0], "a b", "local-operator", ErrInvalidInput},
			{config.TenantID, ids[0], "k", "", ErrInvalidInput},
		} {
			if _, e := r.Replay(ctx, input.tenant, input.id, input.key, input.actor); !errors.Is(e, input.want) {
				t.Fatal(e, input.want)
			}
		}
		if before != finalSnapshot(t, ctx, pool, ids[0]) {
			t.Fatal("foreign mutation")
		}
		replayAuditCount(t, ctx, pool, 0)
	})
	for _, commitFailure := range []bool{false, true} {
		t.Run(fmt.Sprint("rollback ", commitFailure), func(t *testing.T) {
			pool, r, ids := replayFixture(t, ctx, 1)
			before := finalSnapshot(t, ctx, pool, ids[0])
			want := ErrStoreUnavailable
			if commitFailure {
				claimExec(t, ctx, pool, `CREATE FUNCTION test_replay_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test commit failure'; END $$; CREATE CONSTRAINT TRIGGER test_replay_commit_failure AFTER UPDATE ON deliveries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION test_replay_commit_failure()`)
				want = ErrAmbiguousCommit
			} else {
				claimExec(t, ctx, pool, `ALTER TABLE deliveries ADD CONSTRAINT test_replay_failure CHECK(status<>'pending')`)
			}
			if got, e := r.Replay(ctx, config.TenantID, ids[0], "rollback-key", "local-operator"); e != want || got.ReplayID != "" {
				t.Fatal(got, e)
			}
			replayAuditCount(t, ctx, pool, 0)
			if before != finalSnapshot(t, ctx, pool, ids[0]) {
				t.Fatal("partial update")
			}
			if commitFailure {
				claimExec(t, ctx, pool, `DROP TRIGGER test_replay_commit_failure ON deliveries; DROP FUNCTION test_replay_commit_failure()`)
			} else {
				claimExec(t, ctx, pool, `ALTER TABLE deliveries DROP CONSTRAINT test_replay_failure`)
			}
			if _, e := r.Replay(ctx, config.TenantID, ids[0], "rollback-key", "local-operator"); e != nil {
				t.Fatal("key not reusable after rollback", e)
			}
		})
	}
}
