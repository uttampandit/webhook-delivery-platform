//go:build integration

package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SchemaFixtures identifies a valid graph of core and demo rows used by schema tests.
type SchemaFixtures struct {
	TenantA      pgtype.UUID
	TenantB      pgtype.UUID
	DestinationA pgtype.UUID
	DestinationB pgtype.UUID
	EventA       pgtype.UUID
	EventB       pgtype.UUID
	DeliveryA    pgtype.UUID
	AttemptA     pgtype.UUID
	ReplayA      pgtype.UUID
	EffectA      pgtype.UUID
	ScenarioID   string
}

// SeedSchemaFixtures inserts a valid tenant, delivery, attempt, replay, and
// receiver scenario graph into an already-migrated integration database.
func SeedSchemaFixtures(ctx context.Context, pool *pgxpool.Pool) (SchemaFixtures, error) {
	fixtures := SchemaFixtures{
		TenantA:      fixtureUUID("00000000-0000-4000-8000-000000000001"),
		TenantB:      fixtureUUID("00000000-0000-4000-8000-000000000002"),
		DestinationA: fixtureUUID("00000000-0000-4000-8000-000000000011"),
		DestinationB: fixtureUUID("00000000-0000-4000-8000-000000000012"),
		EventA:       fixtureUUID("00000000-0000-4000-8000-000000000021"),
		EventB:       fixtureUUID("00000000-0000-4000-8000-000000000022"),
		DeliveryA:    fixtureUUID("00000000-0000-4000-8000-000000000031"),
		AttemptA:     fixtureUUID("00000000-0000-4000-8000-000000000041"),
		ReplayA:      fixtureUUID("00000000-0000-4000-8000-000000000051"),
		EffectA:      fixtureUUID("00000000-0000-4000-8000-000000000061"),
		ScenarioID:   "schema_fixture_1",
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fixtures, fmt.Errorf("begin schema fixture transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = tx.Rollback(rollbackCtx)
		}
	}()

	requestBody := []byte(`{"event":"order.confirmed"}`)
	requestHash := make([]byte, 32)
	wireBody := []byte(`{"id":"schema-fixture"}`)
	wireHash := make([]byte, 32)
	statement := func(label, query string, args ...any) error {
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("insert %s schema fixture: %w", label, err)
		}
		return nil
	}

	if err := statement("tenants", `
		INSERT INTO tenants (id, name) VALUES ($1, $2), ($3, $4)`,
		fixtures.TenantA, "fixture tenant A", fixtures.TenantB, "fixture tenant B"); err != nil {
		return fixtures, err
	}
	if err := statement("destinations", `
		INSERT INTO destinations (id, tenant_id, code, url, key_ref)
		VALUES ($1, $2, 'fulfillment', 'http://fulfillment.invalid/hook', 'fulfillment-v1'),
		       ($3, $4, 'notification', 'http://notification.invalid/hook', 'notification-v1')`,
		fixtures.DestinationA, fixtures.TenantA, fixtures.DestinationB, fixtures.TenantB); err != nil {
		return fixtures, err
	}
	if err := statement("events", `
		INSERT INTO events (id, tenant_id, idempotency_key, request_body, request_hash, event_type, wire_body)
		VALUES ($1, $2, 'fixture-event-a', $3, $4, 'order.confirmed', $5),
		       ($6, $7, 'fixture-event-b', $3, $4, 'order.confirmed', $5)`,
		fixtures.EventA, fixtures.TenantA, requestBody, requestHash, wireBody,
		fixtures.EventB, fixtures.TenantB); err != nil {
		return fixtures, err
	}
	if err := statement("delivery", `
		INSERT INTO deliveries (
			id, tenant_id, event_id, destination_id, url_snapshot, key_ref_snapshot,
			cycle_attempt_count, lifetime_attempt_count, generation, next_attempt_at
		) VALUES ($1, $2, $3, $4, 'http://fulfillment.invalid/hook', 'fulfillment-v1', 1, 1, 1, CURRENT_TIMESTAMP)`,
		fixtures.DeliveryA, fixtures.TenantA, fixtures.EventA, fixtures.DestinationA); err != nil {
		return fixtures, err
	}
	if err := statement("attempt", `
		INSERT INTO attempts (
			id, delivery_id, attempt_no, cycle_no, cycle_attempt_no, generation,
			claim_token, outcome, finished_at, http_status, duration_ms, retry_delay_ms
		) VALUES ($1, $2, 1, 1, 1, 1, $3, 'http_failure', CURRENT_TIMESTAMP, 503, 4, 100)`,
		fixtures.AttemptA, fixtures.DeliveryA,
		fixtureUUID("00000000-0000-4000-8000-000000000071")); err != nil {
		return fixtures, err
	}
	if err := statement("replay", `
		INSERT INTO replays (id, tenant_id, delivery_id, request_key, actor, from_cycle, to_cycle)
		VALUES ($1, $2, $3, 'fixture-replay-a', 'local-operator', 1, 2)`,
		fixtures.ReplayA, fixtures.TenantA, fixtures.DeliveryA); err != nil {
		return fixtures, err
	}
	if err := statement("demo scenario", `
		INSERT INTO demo_scenarios (receiver_code, scenario_id, mode, dedupe_enabled, drop_remaining)
		VALUES ('fulfillment', $1, 'normal', true, 0)`, fixtures.ScenarioID); err != nil {
		return fixtures, err
	}
	if err := statement("demo request", `
		INSERT INTO demo_requests (receiver_code, scenario_id, operation_id, planned_outcome, business_effect_added)
		VALUES ('fulfillment', $1, $2, 'acknowledge', true)`, fixtures.ScenarioID, fixtures.DeliveryA); err != nil {
		return fixtures, err
	}
	if err := statement("demo dedup", `
		INSERT INTO demo_dedup (receiver_code, scenario_id, operation_id, wire_hash)
		VALUES ('fulfillment', $1, $2, $3)`, fixtures.ScenarioID, fixtures.DeliveryA, wireHash); err != nil {
		return fixtures, err
	}
	if err := statement("demo effect", `
		INSERT INTO demo_effects (id, receiver_code, scenario_id, operation_id, order_id, quantity)
		VALUES ($1, 'fulfillment', $2, $3, 'schema-fixture-order', 2)`,
		fixtures.EffectA, fixtures.ScenarioID, fixtures.DeliveryA); err != nil {
		return fixtures, err
	}
	if err := tx.Commit(ctx); err != nil {
		return fixtures, fmt.Errorf("commit schema fixtures: %w", err)
	}
	committed = true
	return fixtures, nil
}

func fixtureUUID(value string) pgtype.UUID {
	var result pgtype.UUID
	if err := result.Scan(value); err != nil {
		panic(err)
	}
	return result
}
