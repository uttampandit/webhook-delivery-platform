//go:build integration

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestEventAcceptanceRepositoryPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	database, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	applyEventSchema(t, ctx, database.Pool)
	registry, err := config.RegistryForMode(config.RunModeCompose)
	if err != nil {
		t.Fatal("create fixed registry:", err)
	}
	if err := Seed(ctx, database.Pool, registry); err != nil {
		t.Fatal("seed fixed registry:", err)
	}
	tenantID := registry.Tenant().ID
	destinations := registry.Destinations()
	repository := NewEventRepository(database.Pool)

	t.Run("concurrent identical requests and repository restart", func(t *testing.T) {
		const key = "concurrent-identical"
		raw := eventRequestBody(destinations[1].ID, destinations[0].ID)
		request, err := ValidateEventRequest(raw)
		if err != nil {
			t.Fatal("validate request:", err)
		}
		const callers = 20
		type result struct {
			accepted AcceptedEvent
			err      error
		}
		results := make(chan result, callers)
		start := make(chan struct{})
		var wait sync.WaitGroup
		for i := 0; i < callers; i++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				accepted, err := repository.AcceptEvent(ctx, tenantID, key, request)
				results <- result{accepted: accepted, err: err}
			}()
		}
		close(start)
		wait.Wait()
		close(results)

		var first AcceptedEvent
		newCount, duplicateCount := 0, 0
		for result := range results {
			if result.err != nil {
				t.Fatalf("concurrent acceptance failed: %v", result.err)
			}
			if result.accepted.Deduplicated {
				duplicateCount++
			} else {
				newCount++
			}
			if first.EventID == "" {
				first = result.accepted
				continue
			}
			if result.accepted.EventID != first.EventID || !sameStrings(result.accepted.DeliveryIDs, first.DeliveryIDs) || !result.accepted.CreatedAt.Equal(first.CreatedAt) {
				t.Fatalf("concurrent callers returned different acceptance identities: first=%+v other=%+v", first, result.accepted)
			}
		}
		if newCount != 1 || duplicateCount != callers-1 {
			t.Fatalf("new/duplicate responses = %d/%d; want 1/%d", newCount, duplicateCount, callers-1)
		}
		if len(first.DeliveryIDs) != 2 || countEventRows(t, ctx, database.Pool, tenantID, key) != 1 || countDeliveryRows(t, ctx, database.Pool, tenantID, key) != 2 {
			t.Fatalf("accepted event or delivery counts are wrong: result=%+v", first)
		}

		storedID, storedRequest, storedWire, storedCreatedAt := readStoredEvent(t, ctx, database.Pool, tenantID, key)
		if storedID != first.EventID || !bytes.Equal(storedRequest, raw) {
			t.Fatal("stored event identity or raw request differs from the winner")
		}
		var outgoing struct {
			EventID    string `json:"event_id"`
			Type       string `json:"type"`
			OccurredAt string `json:"occurred_at"`
			Data       struct {
				OrderID string `json:"order_id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(storedWire, &outgoing); err != nil {
			t.Fatal("decode persisted outgoing body:", err)
		}
		occurredAt, err := time.Parse(time.RFC3339Nano, outgoing.OccurredAt)
		if err != nil {
			t.Fatal("parse occurred_at:", err)
		}
		if outgoing.EventID != storedID || outgoing.Type != "order.confirmed" || outgoing.Data.OrderID != "order-acceptance" ||
			!occurredAt.Equal(storedCreatedAt) || !first.CreatedAt.Equal(storedCreatedAt) {
			t.Fatalf("outgoing event and persisted timestamp disagree: outgoing=%+v stored=%s result=%s", outgoing, storedCreatedAt, first.CreatedAt)
		}
		var deliveryCreatedAt, deliveryUpdatedAt, nextAttemptAt time.Time
		if err := database.Pool.QueryRow(ctx, `
			SELECT created_at, updated_at, next_attempt_at FROM deliveries WHERE event_id = $1 ORDER BY destination_id LIMIT 1`, storedID).
			Scan(&deliveryCreatedAt, &deliveryUpdatedAt, &nextAttemptAt); err != nil {
			t.Fatal("read initial delivery timestamps:", err)
		}
		if !deliveryCreatedAt.Equal(storedCreatedAt) || !deliveryUpdatedAt.Equal(storedCreatedAt) || !nextAttemptAt.Equal(storedCreatedAt) {
			t.Fatal("initial event and delivery timestamps did not use the same database clock reading")
		}

		// A new repository and pool model an API process restart while preserving
		// the isolated database and its durable event rows.
		restartedPool, err := pgxpool.NewWithConfig(ctx, database.Pool.Config())
		if err != nil {
			t.Fatal("create restarted pool:", err)
		}
		database.Pool.Close()
		database.Pool = restartedPool
		repository = NewEventRepository(restartedPool)
		restartedRepository := NewEventRepository(restartedPool)
		duplicate, err := restartedRepository.AcceptEvent(ctx, tenantID, key, request)
		if err != nil {
			t.Fatal("accept after repository restart:", err)
		}
		_, _, wireAfterRestart, createdAfterRestart := readStoredEvent(t, ctx, restartedPool, tenantID, key)
		if !duplicate.Deduplicated || duplicate.EventID != storedID || !sameStrings(duplicate.DeliveryIDs, first.DeliveryIDs) ||
			!bytes.Equal(wireAfterRestart, storedWire) || !createdAfterRestart.Equal(storedCreatedAt) {
			t.Fatal("duplicate acceptance or repository restart changed the stable event result")
		}
	})

	t.Run("whitespace-only body change conflicts", func(t *testing.T) {
		const key = "whitespace-conflict"
		raw := eventRequestBody(destinations[0].ID)
		request, err := ValidateEventRequest(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repository.AcceptEvent(ctx, tenantID, key, request); err != nil {
			t.Fatal("accept original request:", err)
		}
		changed, err := ValidateEventRequest(append(append([]byte(nil), raw...), '\n'))
		if err != nil {
			t.Fatal("validate whitespace variant:", err)
		}
		if _, err := repository.AcceptEvent(ctx, tenantID, key, changed); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("whitespace change error = %v, want idempotency conflict", err)
		}
		if countEventRows(t, ctx, database.Pool, tenantID, key) != 1 || countDeliveryRows(t, ctx, database.Pool, tenantID, key) != 1 {
			t.Fatal("conflicting whitespace variant created additional rows")
		}
	})

	t.Run("concurrent different bodies have one winner", func(t *testing.T) {
		const key = "concurrent-different"
		firstBody := eventRequestBody(destinations[0].ID)
		secondBody := []byte(strings.Replace(string(firstBody), `"quantity":1`, `"quantity":2`, 1))
		firstRequest, err := ValidateEventRequest(firstBody)
		if err != nil {
			t.Fatal(err)
		}
		secondRequest, err := ValidateEventRequest(secondBody)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wait sync.WaitGroup
		type result struct{ err error }
		results := make(chan result, 2)
		for _, request := range []ValidatedEventRequest{firstRequest, secondRequest} {
			wait.Add(1)
			go func(request ValidatedEventRequest) {
				defer wait.Done()
				<-start
				_, err := repository.AcceptEvent(ctx, tenantID, key, request)
				results <- result{err: err}
			}(request)
		}
		close(start)
		wait.Wait()
		close(results)
		accepted, conflicts := 0, 0
		for result := range results {
			switch {
			case result.err == nil:
				accepted++
			case errors.Is(result.err, ErrIdempotencyConflict):
				conflicts++
			default:
				t.Fatalf("concurrent different-body error = %v", result.err)
			}
		}
		if accepted != 1 || conflicts != 1 || countEventRows(t, ctx, database.Pool, tenantID, key) != 1 || countDeliveryRows(t, ctx, database.Pool, tenantID, key) != 1 {
			t.Fatalf("accepted/conflict counts or persisted rows = %d/%d", accepted, conflicts)
		}
	})

	t.Run("idempotency key is tenant scoped and destinations are tenant scoped", func(t *testing.T) {
		const otherTenantID = "00000000-0000-4000-8000-000000000202"
		const otherDestinationID = "00000000-0000-4000-8000-000000000203"
		if _, err := database.Pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, 'second tenant')`, otherTenantID); err != nil {
			t.Fatal("insert second tenant:", err)
		}
		if _, err := database.Pool.Exec(ctx, `
			INSERT INTO destinations (id, tenant_id, code, url, key_ref)
			VALUES ($1, $2, 'private-test', 'http://127.0.0.1:49125/webhook', 'private-test-v1')`, otherDestinationID, otherTenantID); err != nil {
			t.Fatal("insert second tenant destination:", err)
		}
		const key = "shared-across-tenants"
		first, err := ValidateEventRequest(eventRequestBody(destinations[0].ID))
		if err != nil {
			t.Fatal(err)
		}
		second, err := ValidateEventRequest(eventRequestBody(otherDestinationID))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repository.AcceptEvent(ctx, tenantID, key, first); err != nil {
			t.Fatal("accept in configured tenant:", err)
		}
		if _, err := repository.AcceptEvent(ctx, otherTenantID, key, second); err != nil {
			t.Fatal("same key in another tenant:", err)
		}
		if countEventRows(t, ctx, database.Pool, tenantID, key) != 1 || countEventRows(t, ctx, database.Pool, otherTenantID, key) != 1 {
			t.Fatal("tenant-scoped key did not produce one event in each tenant")
		}

		crossTenant, err := ValidateEventRequest(eventRequestBody(otherDestinationID))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repository.AcceptEvent(ctx, tenantID, "cross-tenant-destination", crossTenant); !errors.Is(err, ErrUnknownDestination) {
			t.Fatalf("cross-tenant destination error = %v, want ErrUnknownDestination", err)
		}
		if countEventRows(t, ctx, database.Pool, tenantID, "cross-tenant-destination") != 0 {
			t.Fatal("cross-tenant destination failure left an event row")
		}
	})

	t.Run("second delivery insert failure rolls back the event", func(t *testing.T) {
		if _, err := database.Pool.Exec(ctx, `
			CREATE FUNCTION fail_second_event_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				IF EXISTS (SELECT 1 FROM deliveries WHERE event_id = NEW.event_id) THEN
					RAISE EXCEPTION 'forced second delivery insert failure' USING ERRCODE = 'P0001';
				END IF;
				RETURN NEW;
			END $$;
			CREATE TRIGGER fail_second_event_delivery BEFORE INSERT ON deliveries
			FOR EACH ROW EXECUTE FUNCTION fail_second_event_delivery()`); err != nil {
			t.Fatal("install second-delivery failure trigger:", err)
		}
		raw := eventRequestBody(destinations[0].ID, destinations[1].ID)
		request, err := ValidateEventRequest(raw)
		if err != nil {
			t.Fatal(err)
		}
		const key = "forced-atomic-rollback"
		if _, err := repository.AcceptEvent(ctx, tenantID, key, request); !errors.Is(err, ErrStoreUnavailable) {
			t.Fatalf("forced delivery failure error = %v, want ErrStoreUnavailable", err)
		}
		if countEventRows(t, ctx, database.Pool, tenantID, key) != 0 || countDeliveryRows(t, ctx, database.Pool, tenantID, key) != 0 {
			t.Fatal("failed second delivery insert left an event or delivery row")
		}
	})

	t.Run("HTML-heavy Unicode remains bounded and unescaped", func(t *testing.T) {
		content := strings.Repeat("<>&雪🙂", 5500)
		raw := []byte(fmt.Sprintf(`{"type":"order.confirmed","payload":{"markup":"%s"},"destination_ids":["%s"]}`, content, destinations[0].ID))
		if len(raw) >= maxEventRequestBytes {
			t.Fatalf("test request length=%d should be bounded below the raw limit", len(raw))
		}
		request, err := ValidateEventRequest(raw)
		if err != nil {
			t.Fatal("validate HTML-heavy Unicode request:", err)
		}
		const key = "html-unicode-bounds"
		accepted, err := repository.AcceptEvent(ctx, tenantID, key, request)
		if err != nil {
			t.Fatal("accept HTML-heavy Unicode request:", err)
		}
		_, _, wire, _ := readStoredEvent(t, ctx, database.Pool, tenantID, key)
		if len(wire) > maxEventWireBytes || !bytes.Contains(wire, []byte("<>&雪🙂")) || bytes.Contains(wire, []byte(`\u003c`)) || bytes.Contains(wire, []byte(`\u0026`)) {
			t.Fatalf("stored wire body was over-expanded, escaped, or lost content: size=%d event=%s", len(wire), accepted.EventID)
		}
	})
}

func eventRequestBody(destinationIDs ...string) []byte {
	quoted := make([]string, len(destinationIDs))
	for i, destinationID := range destinationIDs {
		quoted[i] = `"` + destinationID + `"`
	}
	return []byte(fmt.Sprintf(`{"type":"order.confirmed","payload":{"order_id":"order-acceptance","quantity":1},"destination_ids":[%s]}`, strings.Join(quoted, ",")))
}

func countEventRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID, key string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE tenant_id = $1 AND idempotency_key = $2`, tenantID, key).Scan(&count); err != nil {
		t.Fatal("count event rows:", err)
	}
	return count
}

func countDeliveryRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID, key string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM deliveries d JOIN events e ON e.tenant_id = d.tenant_id AND e.id = d.event_id
		WHERE d.tenant_id = $1 AND e.idempotency_key = $2`, tenantID, key).Scan(&count); err != nil {
		t.Fatal("count delivery rows:", err)
	}
	return count
}

func readStoredEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID, key string) (string, []byte, []byte, time.Time) {
	t.Helper()
	var eventID string
	var requestBody, wireBody []byte
	var createdAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT id::text, request_body, wire_body, created_at
		FROM events WHERE tenant_id = $1 AND idempotency_key = $2`, tenantID, key).
		Scan(&eventID, &requestBody, &wireBody, &createdAt); err != nil {
		t.Fatal("read stored event:", err)
	}
	return eventID, requestBody, wireBody, createdAt.UTC()
}

func sameStrings(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	for i := range first {
		if first[i] != second[i] {
			return false
		}
	}
	return true
}

func applyEventSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate event integration test")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../.."))
	if err := Apply(ctx, pool, filepath.Join(root, "migrations")); err != nil {
		t.Fatal("apply schema for event test:", err)
	}
}
