//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestInitialSchemaAndConstraints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	database, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}

	migrationDir := projectMigrationDir(t)
	if err := Apply(ctx, database.Pool, migrationDir); err != nil {
		t.Fatal("apply initial schema migration:", err)
	}
	var version int64
	var name, checksum string
	var appliedAt time.Time
	if err := database.Pool.QueryRow(ctx, `
		SELECT version, name, sha256, applied_at FROM schema_migrations ORDER BY version`).
		Scan(&version, &name, &checksum, &appliedAt); err != nil {
		t.Fatal("read initial schema migration record:", err)
	}
	if version != 1 || name != "001_initial.sql" || len(checksum) != 64 {
		t.Fatalf("initial migration metadata = (%d, %q, %q); want version 1, 001_initial.sql, 64-character SHA-256", version, name, checksum)
	}
	if err := Apply(ctx, database.Pool, migrationDir); err != nil {
		t.Fatal("repeat initial schema migration:", err)
	}
	var afterCount int
	var afterChecksum string
	var afterAppliedAt time.Time
	if err := database.Pool.QueryRow(ctx, `
		SELECT count(*), min(sha256), min(applied_at) FROM schema_migrations`).
		Scan(&afterCount, &afterChecksum, &afterAppliedAt); err != nil {
		t.Fatal("read migration metadata after repeat:", err)
	}
	if afterCount != 1 || afterChecksum != checksum || !afterAppliedAt.Equal(appliedAt) {
		t.Fatalf("repeat migration changed metadata: count=%d checksum=%q applied_at=%s", afterCount, afterChecksum, afterAppliedAt)
	}

	fixtures, err := testutil.SeedSchemaFixtures(ctx, database.Pool)
	if err != nil {
		t.Fatal("seed valid schema fixtures:", err)
	}

	t.Run("event checks", func(t *testing.T) { testEventChecks(t, ctx, database.Pool, fixtures) })
	t.Run("delivery checks", func(t *testing.T) { testDeliveryChecks(t, ctx, database.Pool, fixtures) })
	t.Run("attempt checks", func(t *testing.T) { testAttemptChecks(t, ctx, database.Pool, fixtures) })
	t.Run("tenant foreign keys and uniqueness", func(t *testing.T) { testTenantReferencesAndUniqueness(t, ctx, database.Pool, fixtures) })
	t.Run("replay checks", func(t *testing.T) { testReplayChecks(t, ctx, database.Pool, fixtures) })
	t.Run("demo checks and duplicate effects", func(t *testing.T) { testDemoChecks(t, ctx, database.Pool, fixtures) })
	t.Run("indexes", func(t *testing.T) { testSchemaIndexes(t, ctx, database.Pool) })
}

func testEventChecks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixtures testutil.SchemaFixtures) {
	t.Helper()
	requestBody := []byte(`{"fixture":true}`)
	requestHash := make([]byte, 32)
	wireBody := []byte(`{"wire":true}`)
	cases := []struct {
		name         string
		key          string
		body         []byte
		requestHash  []byte
		wireBody     []byte
		wantSQLState string
	}{
		{name: "empty idempotency key", key: "", body: requestBody, requestHash: requestHash, wireBody: wireBody, wantSQLState: "23514"},
		{name: "long idempotency key", key: strings.Repeat("k", 129), body: requestBody, requestHash: requestHash, wireBody: wireBody, wantSQLState: "23514"},
		{name: "short request hash", key: "schema-short-hash", body: requestBody, requestHash: make([]byte, 31), wireBody: wireBody, wantSQLState: "23514"},
		{name: "request body over limit", key: "schema-large-request", body: make([]byte, 65537), requestHash: requestHash, wireBody: wireBody, wantSQLState: "23514"},
		{name: "wire body over limit", key: "schema-large-wire", body: requestBody, requestHash: requestHash, wireBody: make([]byte, 98305), wantSQLState: "23514"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expectSQLFailure(t, ctx, pool, tc.wantSQLState, `
				INSERT INTO events (id, tenant_id, idempotency_key, request_body, request_hash, event_type, wire_body)
				VALUES ($1, $2, $3, $4, $5, 'order.confirmed', $6)`,
				schemaTestUUID(uint64(i+100)), fixtures.TenantA, tc.key, tc.body, tc.requestHash, tc.wireBody)
		})
	}
}

func testDeliveryChecks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixtures testutil.SchemaFixtures) {
	t.Helper()
	cases := []struct {
		name         string
		query        string
		wantSQLState string
	}{
		{name: "unknown state", query: `UPDATE deliveries SET status = 'waiting' WHERE id = $1`, wantSQLState: "23514"},
		{name: "null state", query: `UPDATE deliveries SET status = NULL WHERE id = $1`, wantSQLState: "23502"},
		{name: "in-flight without claim token", query: `UPDATE deliveries SET status = 'in_flight', claim_token = NULL, lease_until = CURRENT_TIMESTAMP WHERE id = $1`, wantSQLState: "23514"},
		{name: "in-flight without lease", query: `UPDATE deliveries SET status = 'in_flight', claim_token = $2, lease_until = NULL WHERE id = $1`, wantSQLState: "23514"},
		{name: "pending with claim and lease", query: `UPDATE deliveries SET claim_token = $2, lease_until = CURRENT_TIMESTAMP WHERE id = $1`, wantSQLState: "23514"},
		{name: "zero cycle", query: `UPDATE deliveries SET cycle_no = 0 WHERE id = $1`, wantSQLState: "23514"},
		{name: "negative cycle attempts", query: `UPDATE deliveries SET cycle_attempt_count = -1 WHERE id = $1`, wantSQLState: "23514"},
		{name: "negative lifetime attempts", query: `UPDATE deliveries SET lifetime_attempt_count = -1 WHERE id = $1`, wantSQLState: "23514"},
		{name: "negative generation", query: `UPDATE deliveries SET generation = -1 WHERE id = $1`, wantSQLState: "23514"},
		{name: "max attempts must be five", query: `UPDATE deliveries SET max_attempts = 4 WHERE id = $1`, wantSQLState: "23514"},
		{name: "cycle attempts over limit", query: `UPDATE deliveries SET cycle_attempt_count = 6 WHERE id = $1`, wantSQLState: "23514"},
		{name: "lifetime below cycle attempts", query: `UPDATE deliveries SET lifetime_attempt_count = 0 WHERE id = $1`, wantSQLState: "23514"},
		{name: "succeeded without confirmation", query: `UPDATE deliveries SET status = 'succeeded' WHERE id = $1`, wantSQLState: "23514"},
		{name: "pending with confirmation", query: `UPDATE deliveries SET confirmed_at = CURRENT_TIMESTAMP WHERE id = $1`, wantSQLState: "23514"},
		{name: "dead without terminal reason", query: `UPDATE deliveries SET status = 'dead' WHERE id = $1`, wantSQLState: "23514"},
		{name: "pending with terminal reason", query: `UPDATE deliveries SET terminal_reason = 'unexpected' WHERE id = $1`, wantSQLState: "23514"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := []any{fixtures.DeliveryA}
			if strings.Contains(tc.query, "$2") {
				args = append(args, schemaTestUUID(101))
			}
			expectSQLFailure(t, ctx, pool, tc.wantSQLState, tc.query, args...)
		})
	}
	t.Run("valid in-flight lease", func(t *testing.T) {
		expectSQLSuccessThenRollback(t, ctx, pool,
			`UPDATE deliveries SET status = 'in_flight', claim_token = $2, lease_until = CURRENT_TIMESTAMP WHERE id = $1`,
			fixtures.DeliveryA, schemaTestUUID(102))
	})
	t.Run("valid dead terminal reason", func(t *testing.T) {
		expectSQLSuccessThenRollback(t, ctx, pool,
			`UPDATE deliveries SET status = 'dead', terminal_reason = 'attempt_limit' WHERE id = $1`, fixtures.DeliveryA)
	})
}

func testAttemptChecks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixtures testutil.SchemaFixtures) {
	t.Helper()
	cases := []struct {
		name         string
		query        string
		wantSQLState string
	}{
		{name: "unknown outcome", query: `UPDATE attempts SET outcome = 'success' WHERE id = $1`, wantSQLState: "23514"},
		{name: "zero attempt number", query: `UPDATE attempts SET attempt_no = 0 WHERE id = $1`, wantSQLState: "23514"},
		{name: "zero cycle number", query: `UPDATE attempts SET cycle_no = 0 WHERE id = $1`, wantSQLState: "23514"},
		{name: "zero cycle attempt number", query: `UPDATE attempts SET cycle_attempt_no = 0 WHERE id = $1`, wantSQLState: "23514"},
		{name: "running with finish time", query: `UPDATE attempts SET outcome = 'running', finished_at = CURRENT_TIMESTAMP WHERE id = $1`, wantSQLState: "23514"},
		{name: "finished outcome without finish time", query: `UPDATE attempts SET finished_at = NULL WHERE id = $1`, wantSQLState: "23514"},
		{name: "acknowledged without HTTP status", query: `UPDATE attempts SET outcome = 'acknowledged', http_status = NULL WHERE id = $1`, wantSQLState: "23514"},
		{name: "acknowledged with non-2xx", query: `UPDATE attempts SET outcome = 'acknowledged', http_status = 500 WHERE id = $1`, wantSQLState: "23514"},
		{name: "HTTP failure without status", query: `UPDATE attempts SET outcome = 'http_failure', http_status = NULL WHERE id = $1`, wantSQLState: "23514"},
		{name: "HTTP failure with 2xx", query: `UPDATE attempts SET outcome = 'http_failure', http_status = 204 WHERE id = $1`, wantSQLState: "23514"},
		{name: "status below range", query: `UPDATE attempts SET http_status = 99 WHERE id = $1`, wantSQLState: "23514"},
		{name: "status above range", query: `UPDATE attempts SET http_status = 600 WHERE id = $1`, wantSQLState: "23514"},
		{name: "negative duration", query: `UPDATE attempts SET duration_ms = -1 WHERE id = $1`, wantSQLState: "23514"},
		{name: "negative retry delay", query: `UPDATE attempts SET retry_delay_ms = -1 WHERE id = $1`, wantSQLState: "23514"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expectSQLFailure(t, ctx, pool, tc.wantSQLState, tc.query, fixtures.AttemptA)
		})
	}
	t.Run("valid acknowledgement", func(t *testing.T) {
		expectSQLSuccessThenRollback(t, ctx, pool,
			`UPDATE attempts SET outcome = 'acknowledged', finished_at = CURRENT_TIMESTAMP, http_status = 204 WHERE id = $1`, fixtures.AttemptA)
	})
	t.Run("valid non-2xx failure", func(t *testing.T) {
		expectSQLSuccessThenRollback(t, ctx, pool,
			`UPDATE attempts SET outcome = 'http_failure', finished_at = CURRENT_TIMESTAMP, http_status = 503 WHERE id = $1`, fixtures.AttemptA)
	})
}

func testTenantReferencesAndUniqueness(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixtures testutil.SchemaFixtures) {
	t.Helper()
	for _, tc := range []struct {
		name string
		args []any
	}{
		{
			name: "event from another tenant",
			args: []any{schemaTestUUID(201), fixtures.TenantB, fixtures.EventA, fixtures.DestinationB},
		},
		{
			name: "destination from another tenant",
			args: []any{schemaTestUUID(202), fixtures.TenantB, fixtures.EventB, fixtures.DestinationA},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectSQLFailure(t, ctx, pool, "23503", `
				INSERT INTO deliveries (id, tenant_id, event_id, destination_id, url_snapshot, key_ref_snapshot, next_attempt_at)
				VALUES ($1, $2, $3, $4, 'http://fixture.invalid/hook', 'fixture-key', CURRENT_TIMESTAMP)`, tc.args...)
		})
	}

	t.Run("duplicate destination code per tenant", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23505", `
			INSERT INTO destinations (id, tenant_id, code, url, key_ref)
			VALUES ($1, $2, 'fulfillment', 'http://other.invalid/hook', 'other-key')`,
			schemaTestUUID(203), fixtures.TenantA)
	})
	t.Run("duplicate tenant idempotency key", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23505", `
			INSERT INTO events (id, tenant_id, idempotency_key, request_body, request_hash, event_type, wire_body)
			VALUES ($1, $2, 'fixture-event-a', '{}'::bytea, $3, 'order.confirmed', '{}'::bytea)`,
			schemaTestUUID(204), fixtures.TenantA, make([]byte, 32))
	})
	t.Run("duplicate event destination pair", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23505", `
			INSERT INTO deliveries (id, tenant_id, event_id, destination_id, url_snapshot, key_ref_snapshot, next_attempt_at)
			VALUES ($1, $2, $3, $4, 'http://fixture.invalid/hook', 'fixture-key', CURRENT_TIMESTAMP)`,
			schemaTestUUID(205), fixtures.TenantA, fixtures.EventA, fixtures.DestinationA)
	})
	t.Run("duplicate attempt number", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23505", `
			INSERT INTO attempts (id, delivery_id, attempt_no, cycle_no, cycle_attempt_no, generation, claim_token)
			VALUES ($1, $2, 1, 1, 1, 2, $3)`,
			schemaTestUUID(206), fixtures.DeliveryA, schemaTestUUID(207))
	})
	t.Run("duplicate attempt generation", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23505", `
			INSERT INTO attempts (id, delivery_id, attempt_no, cycle_no, cycle_attempt_no, generation, claim_token)
			VALUES ($1, $2, 2, 1, 1, 1, $3)`,
			schemaTestUUID(208), fixtures.DeliveryA, schemaTestUUID(209))
	})
}

func testReplayChecks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixtures testutil.SchemaFixtures) {
	t.Helper()
	t.Run("from cycle must be positive", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23514", `UPDATE replays SET from_cycle = 0 WHERE id = $1`, fixtures.ReplayA)
	})
	t.Run("to cycle must advance by one", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23514", `UPDATE replays SET to_cycle = 4 WHERE id = $1`, fixtures.ReplayA)
	})
	t.Run("cross-tenant delivery reference", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23503", `
			INSERT INTO replays (id, tenant_id, delivery_id, request_key, actor, from_cycle, to_cycle)
			VALUES ($1, $2, $3, 'cross-tenant-replay', 'local-operator', 2, 3)`,
			schemaTestUUID(301), fixtures.TenantB, fixtures.DeliveryA)
	})
	t.Run("duplicate tenant request key", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23505", `
			INSERT INTO replays (id, tenant_id, delivery_id, request_key, actor, from_cycle, to_cycle)
			VALUES ($1, $2, $3, 'fixture-replay-a', 'local-operator', 2, 3)`,
			schemaTestUUID(302), fixtures.TenantA, fixtures.DeliveryA)
	})
	t.Run("duplicate delivery target cycle", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23505", `
			INSERT INTO replays (id, tenant_id, delivery_id, request_key, actor, from_cycle, to_cycle)
			VALUES ($1, $2, $3, 'another-replay-key', 'local-operator', 1, 2)`,
			schemaTestUUID(303), fixtures.TenantA, fixtures.DeliveryA)
	})
}

func testDemoChecks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixtures testutil.SchemaFixtures) {
	t.Helper()
	scenarioCases := []struct {
		name          string
		receiver      string
		scenarioID    string
		mode          string
		dropRemaining int
	}{
		{name: "unknown receiver", receiver: "warehouse", scenarioID: "unknown-receiver", mode: "normal"},
		{name: "empty scenario id", receiver: "fulfillment", scenarioID: "", mode: "normal"},
		{name: "invalid scenario character", receiver: "fulfillment", scenarioID: "bad/id", mode: "normal"},
		{name: "scenario id too long", receiver: "fulfillment", scenarioID: strings.Repeat("s", 65), mode: "normal"},
		{name: "unknown mode", receiver: "fulfillment", scenarioID: "unknown-mode", mode: "timeout"},
		{name: "drop counter outside range", receiver: "fulfillment", scenarioID: "drop-count", mode: "normal", dropRemaining: 2},
	}
	for _, tc := range scenarioCases {
		t.Run(tc.name, func(t *testing.T) {
			expectSQLFailure(t, ctx, pool, "23514", `
				INSERT INTO demo_scenarios (receiver_code, scenario_id, mode, dedupe_enabled, drop_remaining)
				VALUES ($1, $2, $3, true, $4)`, tc.receiver, tc.scenarioID, tc.mode, tc.dropRemaining)
		})
	}
	t.Run("demo request outcome", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23514", `
			INSERT INTO demo_requests (receiver_code, scenario_id, operation_id, planned_outcome, business_effect_added)
			VALUES ('fulfillment', $1, $2, 'unknown', false)`, fixtures.ScenarioID, fixtures.DeliveryA)
	})
	t.Run("demo request scenario foreign key", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23503", `
			INSERT INTO demo_requests (receiver_code, scenario_id, operation_id, planned_outcome, business_effect_added)
			VALUES ('fulfillment', 'missing-scenario', $1, 'acknowledge', false)`, fixtures.DeliveryA)
	})
	t.Run("demo dedup hash length", func(t *testing.T) {
		expectSQLFailure(t, ctx, pool, "23514", `
			INSERT INTO demo_dedup (receiver_code, scenario_id, operation_id, wire_hash)
			VALUES ('fulfillment', $1, $2, $3)`, fixtures.ScenarioID, fixtures.DeliveryA, make([]byte, 31))
	})
	t.Run("same operation may create multiple effects", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `
			INSERT INTO demo_effects (id, receiver_code, scenario_id, operation_id, order_id, quantity)
			VALUES ($1, 'fulfillment', $2, $3, 'schema-fixture-order-duplicate', 2)`,
			schemaTestUUID(401), fixtures.ScenarioID, fixtures.DeliveryA); err != nil {
			t.Fatal("insert second effect for same operation:", err)
		}
		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM demo_effects
			WHERE receiver_code = 'fulfillment' AND scenario_id = $1 AND operation_id = $2`,
			fixtures.ScenarioID, fixtures.DeliveryA).Scan(&count); err != nil {
			t.Fatal("count duplicate operation effects:", err)
		}
		if count != 2 {
			t.Fatalf("effects for repeated operation = %d; want 2", count)
		}
	})
}

func testSchemaIndexes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	indexes := []struct {
		name       string
		columns    string
		predicate  string
		wantUnique bool
	}{
		{name: "events_tenant_created_id_idx", columns: "(tenant_id,created_atdesc,iddesc)"},
		{name: "deliveries_pending_due_idx", columns: "(next_attempt_at,id)", predicate: "status='pending'::text"},
		{name: "deliveries_in_flight_expired_idx", columns: "(lease_until,id)", predicate: "status='in_flight'::text"},
		{name: "deliveries_tenant_event_id_idx", columns: "(tenant_id,event_id,id)"},
		{name: "attempts_delivery_attempt_no_key", columns: "(delivery_id,attempt_no)", wantUnique: true},
	}
	for _, want := range indexes {
		t.Run(want.name, func(t *testing.T) {
			var definition, predicate string
			var unique, valid bool
			if err := pool.QueryRow(ctx, `
				SELECT pg_get_indexdef(index_class.oid),
				       COALESCE(pg_get_expr(index_meta.indpred, index_meta.indrelid), ''),
				       index_meta.indisunique, index_meta.indisvalid
				FROM pg_index AS index_meta
			JOIN pg_class AS index_class ON index_class.oid = index_meta.indexrelid
			JOIN pg_namespace AS index_schema ON index_schema.oid = index_class.relnamespace
			WHERE index_schema.nspname = 'public' AND index_class.relname = $1`, want.name).
				Scan(&definition, &predicate, &unique, &valid); err != nil {
				t.Fatalf("read index %s: %v", want.name, err)
			}
			normalize := func(value string) string {
				return strings.ToLower(strings.Join(strings.Fields(value), ""))
			}
			if !strings.Contains(normalize(definition), want.columns) {
				t.Errorf("index definition %q does not contain columns %q", definition, want.columns)
			}
			if !strings.Contains(normalize(predicate), want.predicate) {
				t.Errorf("index predicate %q does not contain %q", predicate, want.predicate)
			}
			if unique != want.wantUnique {
				t.Errorf("index unique=%t; want %t", unique, want.wantUnique)
			}
			if !valid {
				t.Errorf("index %s is not valid", want.name)
			}
		})
	}
}

func expectSQLFailure(t *testing.T, ctx context.Context, pool *pgxpool.Pool, wantCode, query string, args ...any) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin constraint assertion:", err)
	}
	_, err = tx.Exec(ctx, query, args...)
	if err == nil {
		rollbackSchemaTestTx(t, tx)
		t.Fatalf("query unexpectedly succeeded; want PostgreSQL SQLSTATE %s", wantCode)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		rollbackSchemaTestTx(t, tx)
		t.Fatalf("query error %T %v; want PostgreSQL SQLSTATE %s", err, err, wantCode)
	}
	if pgErr.Code != wantCode {
		rollbackSchemaTestTx(t, tx)
		t.Fatalf("query SQLSTATE = %s (%s); want %s", pgErr.Code, pgErr.Message, wantCode)
	}
	rollbackSchemaTestTx(t, tx)
}

func expectSQLSuccessThenRollback(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin valid-state assertion:", err)
	}
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		rollbackSchemaTestTx(t, tx)
		t.Fatalf("valid state rejected: %v", err)
	}
	rollbackSchemaTestTx(t, tx)
}

func rollbackSchemaTestTx(t *testing.T, tx pgx.Tx) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Errorf("rollback constraint assertion transaction: %v", err)
	}
}

func projectMigrationDir(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate schema integration test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../migrations"))
}

func schemaTestUUID(suffix uint64) pgtype.UUID {
	var result pgtype.UUID
	value := fmt.Sprintf("10000000-0000-4000-8000-%012x", suffix)
	if err := result.Scan(value); err != nil {
		panic(err)
	}
	return result
}
