//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestEventsReadHTTPIntegrationPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	database, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, database.Pool, testMigrationDir(t)); err != nil {
		t.Fatal("apply application schema:", err)
	}
	settings := testAPISettings(t)
	if err := store.Seed(ctx, database.Pool, settings.Registry()); err != nil {
		t.Fatal("seed application registry:", err)
	}

	repo := store.NewAPIReadRepository(database.Pool)
	handler := NewHandler(settings, repo)

	dests := settings.Registry().Destinations()

	// Ingest 3 events via POST /v1/events
	eventIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		body := []byte(fmt.Sprintf(
			`{"type":"order.confirmed","payload":{"item_no":%d,"order_id":"ord-%d"},"destination_ids":["%s"]}`,
			i+1, i+1, dests[0].ID))
		req := newCreateEventRequest(body, fmt.Sprintf("read-int-key-%d", i+1), true)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("ingest event %d failed: code=%d body=%s", i+1, rec.Code, rec.Body.String())
		}
		var acc struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &acc); err != nil {
			t.Fatal("unmarshal ingest response:", err)
		}
		eventIDs[i] = acc.EventID
	}

	t.Run("list events returns compact items without payload", func(t *testing.T) {
		req := authenticatedRequest(http.MethodGet, "/v1/events?limit=10")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}

		var resp struct {
			Items []map[string]json.RawMessage `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.Items) < 3 {
			t.Fatalf("expected at least 3 items, got %d", len(resp.Items))
		}
		for _, item := range resp.Items {
			if item["payload"] != nil {
				t.Error("list item must not contain payload (gate 4)")
			}
			if item["id"] == nil || item["type"] == nil || item["created_at"] == nil || item["deliveries"] == nil {
				t.Errorf("list item missing standard fields: %+v", item)
			}
		}
	})

	t.Run("pagination with cursor covers all items without duplicates", func(t *testing.T) {
		// Force exact timestamp ties through the actual HTTP cursor encoder/decoder.
		if _, err := database.Pool.Exec(ctx, `UPDATE events SET created_at='2026-01-01T00:00:00Z'`); err != nil {
			t.Fatal(err)
		}
		// Page 1 with limit=2
		req1 := authenticatedRequest(http.MethodGet, "/v1/events?limit=2")
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)
		if rec1.Code != http.StatusOK {
			t.Fatalf("page 1 status = %d", rec1.Code)
		}
		var p1 struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
			NextCursor *string `json:"next_cursor"`
		}
		if err := json.Unmarshal(rec1.Body.Bytes(), &p1); err != nil {
			t.Fatal(err)
		}
		if len(p1.Items) != 2 {
			t.Fatalf("page 1 items = %d, want 2", len(p1.Items))
		}
		if p1.NextCursor == nil || *p1.NextCursor == "" {
			t.Fatal("page 1 next_cursor missing")
		}

		// Page 2 using next_cursor
		req2 := authenticatedRequest(http.MethodGet, "/v1/events?limit=2&cursor="+*p1.NextCursor)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusOK {
			t.Fatalf("page 2 status = %d", rec2.Code)
		}
		var p2 struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
			NextCursor *string `json:"next_cursor"`
		}
		if err := json.Unmarshal(rec2.Body.Bytes(), &p2); err != nil {
			t.Fatal(err)
		}
		if len(p2.Items) != 1 || p2.NextCursor != nil {
			t.Fatalf("page 2 items = %d cursor_present=%t, want 1 and no cursor", len(p2.Items), p2.NextCursor != nil)
		}

		// Check for no overlap between page 1 and page 2
		seen := make(map[string]bool)
		for _, it := range p1.Items {
			seen[it.ID] = true
		}
		for _, it := range p2.Items {
			if seen[it.ID] {
				t.Fatalf("event %s appeared in both page 1 and page 2", it.ID)
			}
		}
	})

	t.Run("event detail returns full DTO with payload and empty arrays", func(t *testing.T) {
		req := authenticatedRequest(http.MethodGet, "/v1/events/"+eventIDs[0])
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("detail status = %d body=%s", rec.Code, rec.Body.String())
		}

		body := rec.Body.String()
		if strings.Contains(body, `"attempts":null`) || strings.Contains(body, `"replays":null`) {
			t.Fatalf("attempts and replays must be [] not null: %s", body)
		}

		var detail struct {
			ID         string                   `json:"id"`
			Type       string                   `json:"type"`
			Payload    map[string]any           `json:"payload"`
			Deliveries []eventDetailDeliveryDTO `json:"deliveries"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
			t.Fatal("decode detail:", err)
		}
		if detail.ID != eventIDs[0] {
			t.Fatalf("detail ID = %q, want %q", detail.ID, eventIDs[0])
		}
		if detail.Payload["order_id"] != "ord-1" {
			t.Errorf("payload order_id = %v, want ord-1", detail.Payload["order_id"])
		}
		if len(detail.Deliveries) != 1 {
			t.Fatalf("deliveries count = %d, want 1", len(detail.Deliveries))
		}
		d := detail.Deliveries[0]
		if d.ID != d.OperationID {
			t.Errorf("id %q != operation_id %q", d.ID, d.OperationID)
		}
		if d.Attempts == nil {
			t.Error("attempts slice should not be nil")
		}
		if d.Replays == nil {
			t.Error("replays slice should not be nil")
		}
	})

	t.Run("unknown event ID returns 404", func(t *testing.T) {
		req := authenticatedRequest(http.MethodGet, "/v1/events/00000000-0000-4000-8000-000000009999")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unknown ID status = %d, want 404", rec.Code)
		}
		var errResp errorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errResp)
		if errResp.Error.Code != codeNotFound {
			t.Errorf("error code = %q, want not_found", errResp.Error.Code)
		}
	})

	t.Run("cross-tenant isolation: foreign tenant cannot access events", func(t *testing.T) {
		// Insert an event directly for a foreign tenant in PostgreSQL
		foreignTenantID := "00000000-0000-4000-8000-000000000002"
		foreignEventID := "00000000-0000-4000-8000-000000000022"
		_, err := database.Pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $2)`, foreignTenantID, "foreign")
		if err != nil {
			t.Fatal(err)
		}
		_, err = database.Pool.Exec(ctx, `
			INSERT INTO events (id, tenant_id, idempotency_key, request_body, request_hash, event_type, wire_body, created_at)
			VALUES ($1, $2, 'foreign-key', '{"type":"order.confirmed","payload":{},"destination_ids":[]}', $3, 'order.confirmed', '{"data":{}}', CURRENT_TIMESTAMP)`,
			foreignEventID, foreignTenantID, make([]byte, 32))
		if err != nil {
			t.Fatal(err)
		}

		// When the operator (scoped to demo tenant) requests the foreign event, it must 404
		req := authenticatedRequest(http.MethodGet, "/v1/events/"+foreignEventID)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("foreign event lookup code = %d, want 404", rec.Code)
		}

		// Spoofed tenant headers in request must be ignored by authenticated operator handler
		reqSpoof := authenticatedRequest(http.MethodGet, "/v1/events/"+foreignEventID)
		reqSpoof.Header.Set("X-Tenant-ID", foreignTenantID)
		recSpoof := httptest.NewRecorder()
		handler.ServeHTTP(recSpoof, reqSpoof)
		if recSpoof.Code != http.StatusNotFound {
			t.Fatalf("spoofed header foreign event lookup code = %d, want 404", recSpoof.Code)
		}
	})
	t.Run("real database private snapshots stay out of authenticated HTTP", func(t *testing.T) {
		const privateMarker = "planted-private-snapshot-marker"
		if _, err := database.Pool.Exec(ctx, `UPDATE deliveries SET url_snapshot=$1,key_ref_snapshot=$2`, "http://user:"+privateMarker+"@127.0.0.1/private", privateMarker); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"/v1/events", "/v1/events/" + eventIDs[0], "/v1/destinations"} {
			authorized := httptest.NewRecorder()
			handler.ServeHTTP(authorized, authenticatedRequest(http.MethodGet, path))
			if authorized.Code != 200 {
				t.Fatalf("status=%d", authorized.Code)
			}
			body := authorized.Body.String()
			for _, secret := range []string{privateMarker, settings.APIToken(), "url_snapshot", "key_ref_snapshot", "claim_token", "wire_body", "request_hash"} {
				if strings.Contains(body, secret) {
					t.Fatal("private field/value appeared in public HTTP DTO")
				}
			}
			denied := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, path, nil)
			request.Header.Set("Authorization", "Bearer "+privateMarker)
			handler.ServeHTTP(denied, request)
			if denied.Code != 401 || strings.Contains(denied.Body.String(), privateMarker) {
				t.Fatal("unauthorized response exposed input or data")
			}
		}
	})
}
