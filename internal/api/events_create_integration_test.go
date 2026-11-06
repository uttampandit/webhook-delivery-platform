//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestCreateEventLostResponseResendUsesPersistedIdentity(t *testing.T) {
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
	registryDestinations := settings.Registry().Destinations()
	body := []byte(fmt.Sprintf(
		`{"type":"order.confirmed","payload":{"scenario_id":"api_loss","order_id":"order_loss","quantity":3},"destination_ids":["%s","%s"]}`,
		registryDestinations[1].ID, registryDestinations[0].ID))
	const key = "api-response-lost-after-commit"
	handler := NewHandler(settings, store.NewAPIReadRepository(database.Pool))

	// The store commits before the handler starts writing. This writer returns a
	// closed-connection error to model a response the client never receives.
	lostResponse := &disconnectedEventResponse{header: make(http.Header)}
	firstRequest := newCreateEventRequest(body, key, true)
	firstRequest.Header.Set("X-Tenant-ID", "00000000-0000-4000-8000-000000000099")
	firstRequest.Header.Set("X-Actor", "attacker-selected-actor")
	handler.ServeHTTP(lostResponse, firstRequest)
	if lostResponse.status != http.StatusAccepted {
		t.Fatalf("first response status before connection loss = %d, want 202", lostResponse.status)
	}

	resend := httptest.NewRecorder()
	secondRequest := newCreateEventRequest(body, key, true)
	handler.ServeHTTP(resend, secondRequest)
	if resend.Code != http.StatusOK {
		t.Fatalf("identical resend status = %d body=%q, want 200", resend.Code, resend.Body.String())
	}
	var response createEventResponse
	if err := json.Unmarshal(resend.Body.Bytes(), &response); err != nil {
		t.Fatal("decode duplicate response:", err)
	}
	if !response.Deduplicated || response.EventID == "" || len(response.DeliveryIDs) != 2 {
		t.Fatalf("duplicate response = %+v", response)
	}
	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, newCreateEventRequest(append(append([]byte(nil), body...), '\n'), key, true))
	assertCreateEventError(t, conflict, http.StatusConflict, codeIdempotencyConflict)

	tenantID := settings.Registry().Tenant().ID
	var eventCount, deliveryCount int
	if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE tenant_id = $1 AND idempotency_key = $2`, tenantID, key).Scan(&eventCount); err != nil {
		t.Fatal("count accepted events:", err)
	}
	if err := database.Pool.QueryRow(ctx, `
		SELECT count(*) FROM deliveries d JOIN events e ON e.tenant_id = d.tenant_id AND e.id = d.event_id
		WHERE e.tenant_id = $1 AND e.idempotency_key = $2`, tenantID, key).Scan(&deliveryCount); err != nil {
		t.Fatal("count accepted deliveries:", err)
	}
	if eventCount != 1 || deliveryCount != 2 {
		t.Fatalf("after response loss and retry, events=%d deliveries=%d; want 1 and 2", eventCount, deliveryCount)
	}
	rows, err := database.Pool.Query(ctx, `
		SELECT d.id::text FROM deliveries d
		JOIN events e ON e.tenant_id = d.tenant_id AND e.id = d.event_id
		WHERE e.tenant_id = $1 AND e.idempotency_key = $2
		ORDER BY d.destination_id`, tenantID, key)
	if err != nil {
		t.Fatal("read deterministic delivery order:", err)
	}
	var orderedDeliveryIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal("scan delivery ID:", err)
		}
		orderedDeliveryIDs = append(orderedDeliveryIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal("read delivery IDs:", err)
	}
	rows.Close()
	if len(orderedDeliveryIDs) != len(response.DeliveryIDs) {
		t.Fatalf("ordered delivery IDs=%v, response IDs=%v", orderedDeliveryIDs, response.DeliveryIDs)
	}
	for i := range orderedDeliveryIDs {
		if orderedDeliveryIDs[i] != response.DeliveryIDs[i] {
			t.Fatalf("delivery response order=%v; database order=%v", response.DeliveryIDs, orderedDeliveryIDs)
		}
	}
	var persistedWire []byte
	if err := database.Pool.QueryRow(ctx, `SELECT wire_body FROM events WHERE tenant_id = $1 AND id = $2`, tenantID, response.EventID).Scan(&persistedWire); err != nil {
		t.Fatal("read persisted outgoing body:", err)
	}
	var outgoing struct {
		EventID string `json:"event_id"`
		Data    struct {
			OrderID string `json:"order_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(persistedWire, &outgoing); err != nil {
		t.Fatal("decode persisted outgoing body:", err)
	}
	if outgoing.EventID != response.EventID || outgoing.Data.OrderID != "order_loss" {
		t.Fatalf("persisted event does not match resend response: %+v", outgoing)
	}
}

func TestCreateEventOversizedBodiesPersistNothingInPostgreSQL(t *testing.T) {
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
	handler := NewHandler(settings, store.NewAPIReadRepository(database.Pool))
	destinationID := settings.Registry().Destinations()[0].ID
	body := []byte(fmt.Sprintf(
		`{"type":"order.confirmed","payload":{"large":"%s"},"destination_ids":["%s"]}`,
		strings.Repeat("x", store.MaxEventRequestBytes), destinationID))
	if len(body) <= store.MaxEventRequestBytes {
		t.Fatalf("oversized fixture is only %d bytes", len(body))
	}

	for _, test := range []struct {
		name          string
		contentLength int64
		key           string
	}{
		{name: "content length absent", contentLength: -1, key: "oversize-no-length"},
		{name: "content length understated", contentLength: 1, key: "oversize-false-length"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := newCreateEventRequest(body, test.key, true)
			request.ContentLength = test.contentLength
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			assertCreateEventError(t, recorder, http.StatusRequestEntityTooLarge, codeBodyTooLarge)
			var count int
			if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE tenant_id = $1 AND idempotency_key = $2`, settings.Registry().Tenant().ID, test.key).Scan(&count); err != nil {
				t.Fatal("count persisted oversized event:", err)
			}
			if count != 0 {
				t.Fatalf("oversized request persisted %d events", count)
			}
		})
	}
}

func TestCreateEventUnknownDestinationPersistsNothingInPostgreSQL(t *testing.T) {
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
	handler := NewHandler(settings, store.NewAPIReadRepository(database.Pool))
	const unknownDestination = "00000000-0000-4000-8000-000000000199"
	body := []byte(fmt.Sprintf(`{"type":"order.confirmed","payload":{},"destination_ids":["%s"]}`, unknownDestination))
	const key = "unknown-destination-no-write"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, newCreateEventRequest(body, key, true))
	assertCreateEventError(t, recorder, http.StatusBadRequest, codeUnknownDestination)
	var count int
	if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE tenant_id = $1 AND idempotency_key = $2`, settings.Registry().Tenant().ID, key).Scan(&count); err != nil {
		t.Fatal("count event after unknown destination:", err)
	}
	if count != 0 {
		t.Fatalf("unknown destination persisted %d event rows", count)
	}
}

type disconnectedEventResponse struct {
	header http.Header
	status int
}

func (writer *disconnectedEventResponse) Header() http.Header { return writer.header }

func (writer *disconnectedEventResponse) WriteHeader(status int) { writer.status = status }

func (writer *disconnectedEventResponse) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
