//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestQueryRepositoryPostgreSQL(t *testing.T) {
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
		t.Fatal("seed:", err)
	}

	tenantID := registry.Tenant().ID
	destinations := registry.Destinations()
	eventRepo := NewEventRepository(database.Pool)
	queryRepo := NewQueryRepository(database.Pool)

	// Accept two events for the tenant so we can test list ordering and pagination.
	req1, err := ValidateEventRequest(eventRequestBody(destinations[0].ID))
	if err != nil {
		t.Fatal("validate req1:", err)
	}
	accepted1, err := eventRepo.AcceptEvent(ctx, tenantID, "query-integration-key-1", req1)
	if err != nil {
		t.Fatal("accept event 1:", err)
	}

	req2, err := ValidateEventRequest(eventRequestBody(destinations[1].ID))
	if err != nil {
		t.Fatal("validate req2:", err)
	}
	accepted2, err := eventRepo.AcceptEvent(ctx, tenantID, "query-integration-key-2", req2)
	if err != nil {
		t.Fatal("accept event 2:", err)
	}

	t.Run("list returns events newest-first", func(t *testing.T) {
		items, nextCursor, err := queryRepo.ListEvents(ctx, tenantID, 10, nil)
		if err != nil {
			t.Fatal("list events:", err)
		}
		if nextCursor != nil {
			t.Error("next_cursor should be nil for a full page")
		}
		found1, found2 := false, false
		for _, item := range items {
			if item.ID == accepted1.EventID {
				found1 = true
			}
			if item.ID == accepted2.EventID {
				found2 = true
			}
		}
		if !found1 || !found2 {
			t.Fatalf("list missing expected events: found1=%v found2=%v items=%d", found1, found2, len(items))
		}
		// accepted2 was inserted after accepted1, so it should come first.
		idx1, idx2 := -1, -1
		for i, item := range items {
			if item.ID == accepted1.EventID {
				idx1 = i
			}
			if item.ID == accepted2.EventID {
				idx2 = i
			}
		}
		if idx1 < idx2 {
			t.Errorf("event1 (idx=%d) appeared before event2 (idx=%d); want newest-first", idx1, idx2)
		}
	})

	t.Run("list includes non-nil deliveries array", func(t *testing.T) {
		items, _, err := queryRepo.ListEvents(ctx, tenantID, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if item.Deliveries == nil {
				t.Fatalf("event %s has nil deliveries slice", item.ID)
			}
		}
	})

	t.Run("list does not fetch payload (gate 4)", func(t *testing.T) {
		// EventListItem type has no Payload field — verify by marshalling to a raw map.
		items, _, err := queryRepo.ListEvents(ctx, tenantID, 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) == 0 {
			t.Fatal("expected at least one item")
		}
		raw, _ := json.Marshal(items[0])
		var m map[string]json.RawMessage
		_ = json.Unmarshal(raw, &m)
		if m["Payload"] != nil || m["payload"] != nil {
			t.Error("list item type must not expose payload")
		}
	})

	t.Run("equal-timestamp pagination covers each event exactly once", func(t *testing.T) {
		// Insert 5 events with an identical timestamp to test UUID tie-breaking.
		fixedTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		tenantUUID, _ := parseCanonicalUUID(tenantID)
		destUUID, _ := parseCanonicalUUID(destinations[0].ID)

		insertedIDs := make([]string, 5)
		for i := 0; i < 5; i++ {
			evID, evUUID, err := newEventUUID()
			if err != nil {
				t.Fatal(err)
			}
			_, delivUUID, err := newEventUUID()
			if err != nil {
				t.Fatal(err)
			}
			tx, err := database.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO events (id, tenant_id, idempotency_key, request_body, request_hash, event_type, wire_body, created_at)
				VALUES ($1, $2, $3, $4, $5, 'order.confirmed', $6, $7)`,
				evUUID, tenantUUID, "pagination-tie-"+evID,
				[]byte(`{"type":"order.confirmed","payload":{},"destination_ids":[]}`),
				make([]byte, 32),
				[]byte(`{"event_id":"`+evID+`","type":"order.confirmed","occurred_at":"2026-01-01T00:00:00Z","data":{}}`),
				fixedTime)
			if err != nil {
				_ = tx.Rollback(ctx)
				t.Fatal("insert event:", err)
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO deliveries (id, tenant_id, event_id, destination_id, url_snapshot, key_ref_snapshot, next_attempt_at, created_at, updated_at)
				VALUES ($1, $2, $3, $4, 'http://test.invalid/hook', 'test-v1', $5, $5, $5)`,
				delivUUID, tenantUUID, evUUID, destUUID, fixedTime)
			if err != nil {
				_ = tx.Rollback(ctx)
				t.Fatal("insert delivery:", err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal("commit:", err)
			}
			insertedIDs[i] = evID
		}

		// Page through with limit=2 and collect all IDs seen.
		seen := make(map[string]int)
		var cursor *EventCursor
		pageCount := 0
		for {
			items, nextCursor, err := queryRepo.ListEvents(ctx, tenantID, 2, cursor)
			if err != nil {
				t.Fatal("list page:", err)
			}
			pageCount++
			for _, item := range items {
				seen[item.ID]++
			}
			if nextCursor == nil {
				break
			}
			cursor = nextCursor
			if pageCount > 20 {
				t.Fatal("pagination did not terminate")
			}
		}

		// Every inserted equal-timestamp event must appear exactly once.
		for _, id := range insertedIDs {
			if count := seen[id]; count != 1 {
				t.Errorf("event %s appeared %d times across pages, want exactly 1", id, count)
			}
		}
	})

	t.Run("cursor round-trip: page 2 has no overlap with page 1", func(t *testing.T) {
		items1, next1, err := queryRepo.ListEvents(ctx, tenantID, 1, nil)
		if err != nil || len(items1) == 0 {
			t.Fatal("page 1:", err)
		}
		if next1 == nil {
			t.Skip("only one event in DB; cursor test needs at least 2")
		}
		encoded, err := EncodeCursor(*next1)
		if err != nil {
			t.Fatal("encode cursor:", err)
		}
		decoded, err := DecodeCursor(encoded)
		if err != nil {
			t.Fatal("decode cursor:", err)
		}
		items2, _, err := queryRepo.ListEvents(ctx, tenantID, 1, &decoded)
		if err != nil {
			t.Fatal("page 2:", err)
		}
		for _, item1 := range items1 {
			for _, item2 := range items2 {
				if item1.ID == item2.ID {
					t.Errorf("event %s appeared in both page 1 and page 2", item1.ID)
				}
			}
		}
	})

	t.Run("invalid limit returns ErrInvalidLimit", func(t *testing.T) {
		_, _, err := queryRepo.ListEvents(ctx, tenantID, 0, nil)
		if !errors.Is(err, ErrInvalidLimit) {
			t.Fatalf("limit=0 error = %v, want ErrInvalidLimit", err)
		}
		_, _, err = queryRepo.ListEvents(ctx, tenantID, 101, nil)
		if !errors.Is(err, ErrInvalidLimit) {
			t.Fatalf("limit=101 error = %v, want ErrInvalidLimit", err)
		}
	})

	t.Run("foreign tenant list returns empty slice", func(t *testing.T) {
		otherTenantID := "00000000-0000-4000-8000-000000000099"
		items, _, err := queryRepo.ListEvents(ctx, otherTenantID, 10, nil)
		if err != nil {
			t.Fatal("list other tenant:", err)
		}
		if len(items) != 0 {
			t.Errorf("foreign tenant list returned %d items, want 0", len(items))
		}
	})

	t.Run("get event detail includes payload and delivery fields", func(t *testing.T) {
		detail, err := queryRepo.GetEvent(ctx, tenantID, accepted1.EventID)
		if err != nil {
			t.Fatal("get event:", err)
		}
		if detail.ID != accepted1.EventID {
			t.Fatalf("id = %q, want %q", detail.ID, accepted1.EventID)
		}
		if detail.Type == "" {
			t.Error("event type is empty")
		}
		if detail.Payload == nil || string(detail.Payload) == "null" {
			t.Error("payload must be non-nil")
		}
		if detail.Deliveries == nil {
			t.Error("deliveries must not be nil")
		}
		if len(detail.Deliveries) != 1 {
			t.Errorf("deliveries count = %d, want 1", len(detail.Deliveries))
		}
		d := detail.Deliveries[0]
		if d.ID == "" || d.OperationID == "" || d.ID != d.OperationID {
			t.Errorf("delivery id=%q operation_id=%q must be equal and non-empty", d.ID, d.OperationID)
		}
		if d.Attempts == nil {
			t.Error("attempts must not be nil")
		}
		if d.Replays == nil {
			t.Error("replays must not be nil")
		}
	})

	t.Run("get event detail does not expose key_ref or url_snapshot", func(t *testing.T) {
		detail, err := queryRepo.GetEvent(ctx, tenantID, accepted1.EventID)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(detail)
		body := string(raw)
		for _, banned := range []string{"key_ref", "url_snapshot", "request_body", "request_hash", "wire_body", "token"} {
			if strings.Contains(body, `"`+banned+`"`) {
				t.Errorf("event detail exposed banned field %q", banned)
			}
		}
	})

	t.Run("get event detail for foreign tenant returns ErrNotFound", func(t *testing.T) {
		otherTenantID := "00000000-0000-4000-8000-000000000099"
		_, err := queryRepo.GetEvent(ctx, otherTenantID, accepted1.EventID)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("foreign tenant detail = %v, want ErrNotFound", err)
		}
	})

	t.Run("get event detail for unknown ID returns ErrNotFound", func(t *testing.T) {
		_, err := queryRepo.GetEvent(ctx, tenantID, "00000000-0000-4000-8000-000000009999")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("unknown ID detail = %v, want ErrNotFound", err)
		}
	})

	t.Run("get event detail payload derives from wire body data field only", func(t *testing.T) {
		detail, err := queryRepo.GetEvent(ctx, tenantID, accepted1.EventID)
		if err != nil {
			t.Fatal(err)
		}
		// Payload should contain the original payload data, not wire-level envelope.
		body := string(detail.Payload)
		for _, wireField := range []string{"event_id", "occurred_at"} {
			if strings.Contains(body, `"`+wireField+`"`) {
				t.Errorf("payload exposed wire body field %q (should only be data sub-object)", wireField)
			}
		}
	})
}
