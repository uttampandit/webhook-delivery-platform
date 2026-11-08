package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

// --- Helpers ---

func authenticatedRequest(method, path string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	r.Header.Set("Authorization", "Bearer "+testAPIToken)
	return r
}

// --- GET /v1/events (list) ---

func TestListEventsReturnsEmptyItemsAndNullCursor(t *testing.T) {
	repository := &testRepository{}
	handler := NewHandler(testSettings(t), repository)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, "/v1/events"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s, want 200", recorder.Code, recorder.Body.String())
	}

	var resp struct {
		Items      []json.RawMessage `json:"items"`
		NextCursor *string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatal("decode list response:", err)
	}
	if len(resp.Items) != 0 {
		t.Fatalf("items count = %d, want 0", len(resp.Items))
	}
	if resp.NextCursor != nil {
		t.Fatalf("next_cursor = %q, want null", *resp.NextCursor)
	}
}

func TestListEventsItemDTOFieldNames(t *testing.T) {
	createdAt := time.Date(2026, 11, 6, 8, 10, 11, 0, time.UTC)
	repository := &testRepository{
		listEventsItems: []store.EventListItem{
			{
				ID:        "00000000-0000-4000-8000-000000000001",
				Type:      "order.confirmed",
				CreatedAt: createdAt,
				Deliveries: []store.DeliveryListItem{
					{
						ID:                     "00000000-0000-4000-8000-000000000011",
						DestinationCode:        "fulfillment",
						Status:                 "pending",
						HasUnconfirmedAttempts: false,
					},
				},
			},
		},
	}
	handler := NewHandler(testSettings(t), repository)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, "/v1/events"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s, want 200", recorder.Code, recorder.Body.String())
	}

	// Unmarshal to raw maps to verify exact field names.
	var resp struct {
		Items []map[string]json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatal("decode:", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(resp.Items))
	}
	item := resp.Items[0]
	for _, required := range []string{"id", "type", "created_at", "deliveries"} {
		if item[required] == nil {
			t.Errorf("event list item missing field %q", required)
		}
	}
	// Payload must NOT be in list items.
	if item["payload"] != nil {
		t.Error("event list item must not include payload")
	}

	var deliveries []map[string]json.RawMessage
	if err := json.Unmarshal(item["deliveries"], &deliveries); err != nil || len(deliveries) != 1 {
		t.Fatalf("deliveries decode failed or count wrong: %v", err)
	}
	for _, required := range []string{"id", "destination_code", "status", "has_unconfirmed_attempts"} {
		if deliveries[0][required] == nil {
			t.Errorf("delivery list item missing field %q", required)
		}
	}
	// Sensitive fields must not appear.
	for _, banned := range []string{"url", "key_ref", "url_snapshot", "key_ref_snapshot", "token"} {
		if deliveries[0][banned] != nil {
			t.Errorf("delivery list item exposed banned field %q", banned)
		}
	}
}

func TestListEventsRequiresAuth(t *testing.T) {
	handler := NewHandler(testSettings(t), &testRepository{})
	recorder := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	handler.ServeHTTP(recorder, r)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated list = %d, want 401", recorder.Code)
	}
}

func TestListEventsRejectsInvalidLimitAndCursor(t *testing.T) {
	handler := NewHandler(testSettings(t), &testRepository{})

	for _, query := range []string{
		"?limit=0",
		"?limit=101",
		"?limit=abc",
		"?limit=-1",
		"?cursor=!!!invalid!!!",
		"?cursor=" + strings.Repeat("x", 513), // over MaxCursorBytes
	} {
		t.Run(query, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, "/v1/events"+query))
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("%s status = %d body=%s, want 400", query, recorder.Code, recorder.Body.String())
			}
			var resp errorEnvelope
			if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
				t.Fatal("decode error envelope:", err)
			}
			if resp.Error.Code != codeInvalidRequest {
				t.Fatalf("error code = %q, want invalid_request", resp.Error.Code)
			}
		})
	}
}

func TestListEventsRepositoryErrorMaps503(t *testing.T) {
	repository := &testRepository{listEventsError: store.ErrStoreUnavailable}
	handler := NewHandler(testSettings(t), repository)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, "/v1/events"))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("store error = %d, want 503", recorder.Code)
	}
}

func TestListEventsMethodNotAllowedForUnsupportedMethods(t *testing.T) {
	handler := NewHandler(testSettings(t), &testRepository{})
	for _, method := range []string{http.MethodDelete, http.MethodPut, http.MethodPatch} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, authenticatedRequest(method, "/v1/events"))
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /v1/events = %d, want 405", method, recorder.Code)
		}
	}
}

// --- GET /v1/events/{id} (detail) ---

func TestGetEventDetailDTOFieldNames(t *testing.T) {
	createdAt := time.Date(2026, 11, 6, 8, 10, 11, 0, time.UTC)
	startedAt := time.Date(2026, 11, 6, 8, 10, 12, 0, time.UTC)
	finishedAt := time.Date(2026, 11, 6, 8, 10, 13, 0, time.UTC)
	status503 := 503
	errCode := "http_failure"
	dur := int64(45)
	delay := int64(1000)

	replayCreatedAt := time.Date(2026, 11, 6, 8, 11, 0, 0, time.UTC)

	repository := &testRepository{
		getEventDetail: store.EventDetail{
			ID:        "00000000-0000-4000-8000-000000000001",
			Type:      "order.confirmed",
			CreatedAt: createdAt,
			Payload:   json.RawMessage(`{"order_id":"ord-1","quantity":2}`),
			Deliveries: []store.DeliveryDetail{
				{
					ID:                     "00000000-0000-4000-8000-000000000011",
					OperationID:            "00000000-0000-4000-8000-000000000011",
					DestinationCode:        "fulfillment",
					Status:                 "pending",
					CycleNo:                1,
					CycleAttemptCount:      1,
					LifetimeAttemptCount:   1,
					MaxAttempts:            5,
					HasUnconfirmedAttempts: false,
					Attempts: []store.AttemptDetail{
						{
							ID:             "00000000-0000-4000-8000-000000000021",
							AttemptNo:      1,
							CycleNo:        1,
							CycleAttemptNo: 1,
							Outcome:        "http_failure",
							StartedAt:      startedAt,
							FinishedAt:     &finishedAt,
							HTTPStatus:     &status503,
							ErrorCode:      &errCode,
							DurationMs:     &dur,
							RetryDelayMs:   &delay,
						},
					},
					Replays: []store.ReplayDetail{
						{
							ID:        "00000000-0000-4000-8000-000000000031",
							FromCycle: 1,
							ToCycle:   2,
							Actor:     "local-operator",
							CreatedAt: replayCreatedAt,
						},
					},
				},
			},
		},
	}
	handler := NewHandler(testSettings(t), repository)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, "/v1/events/00000000-0000-4000-8000-000000000001"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s, want 200", recorder.Code, recorder.Body.String())
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &raw); err != nil {
		t.Fatal("decode:", err)
	}
	for _, required := range []string{"id", "type", "created_at", "payload", "deliveries"} {
		if raw[required] == nil {
			t.Errorf("event detail missing field %q", required)
		}
	}
	// Sensitive fields must never appear in event detail.
	for _, banned := range []string{"request_body", "request_hash", "wire_body", "url_snapshot", "key_ref", "key_ref_snapshot", "token"} {
		if raw[banned] != nil {
			t.Errorf("event detail exposed banned field %q", banned)
		}
	}

	var deliveries []map[string]json.RawMessage
	if err := json.Unmarshal(raw["deliveries"], &deliveries); err != nil || len(deliveries) != 1 {
		t.Fatalf("deliveries: %v", err)
	}
	d := deliveries[0]
	for _, required := range []string{
		"id", "operation_id", "destination_code", "status",
		"cycle_no", "cycle_attempt_count", "lifetime_attempt_count", "max_attempts",
		"next_attempt_at", "lease_until", "confirmed_at", "terminal_reason",
		"has_unconfirmed_attempts", "attempts", "replays",
	} {
		if d[required] == nil {
			t.Errorf("delivery detail missing field %q", required)
		}
	}
	// id and operation_id should be equal.
	var delivID, opID string
	if err := json.Unmarshal(d["id"], &delivID); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(d["operation_id"], &opID); err != nil {
		t.Fatal(err)
	}
	if delivID != opID {
		t.Fatalf("id %q != operation_id %q", delivID, opID)
	}

	var attempts []map[string]json.RawMessage
	if err := json.Unmarshal(d["attempts"], &attempts); err != nil || len(attempts) != 1 {
		t.Fatalf("attempts: %v", err)
	}
	for _, required := range []string{
		"id", "attempt_no", "cycle_no", "cycle_attempt_no", "outcome",
		"started_at", "finished_at", "http_status", "error_code", "duration_ms", "retry_delay_ms",
	} {
		if attempts[0][required] == nil {
			t.Errorf("attempt missing field %q", required)
		}
	}

	var replays []map[string]json.RawMessage
	if err := json.Unmarshal(d["replays"], &replays); err != nil || len(replays) != 1 {
		t.Fatalf("replays: %v", err)
	}
	for _, required := range []string{"id", "from_cycle", "to_cycle", "actor", "created_at"} {
		if replays[0][required] == nil {
			t.Errorf("replay missing field %q", required)
		}
	}
	// Replay must NOT expose request_key or other internal fields.
	for _, banned := range []string{"request_key", "tenant_id", "delivery_id"} {
		if replays[0][banned] != nil {
			t.Errorf("replay exposed banned field %q", banned)
		}
	}
}

func TestGetEventDetailEmptyAttemptsAndReplaysAreArrays(t *testing.T) {
	repository := &testRepository{
		getEventDetail: store.EventDetail{
			ID:        "00000000-0000-4000-8000-000000000001",
			Type:      "order.confirmed",
			CreatedAt: time.Now().UTC(),
			Payload:   json.RawMessage(`{}`),
			Deliveries: []store.DeliveryDetail{
				{
					ID:          "00000000-0000-4000-8000-000000000011",
					OperationID: "00000000-0000-4000-8000-000000000011",
					Status:      "pending",
					MaxAttempts: 5,
					Attempts:    []store.AttemptDetail{},
					Replays:     []store.ReplayDetail{},
				},
			},
		},
	}
	handler := NewHandler(testSettings(t), repository)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, "/v1/events/00000000-0000-4000-8000-000000000001"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	// Attempts and replays must be JSON arrays, not null.
	if strings.Contains(body, `"attempts":null`) || strings.Contains(body, `"replays":null`) {
		t.Fatalf("empty attempts/replays must be [] not null: %s", body)
	}
}

func TestGetEventDetailNullableTimestamps(t *testing.T) {
	// A pending delivery has next_attempt_at set, confirmed_at/terminal_reason null.
	nextAttemptAt := time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC)
	repository := &testRepository{
		getEventDetail: store.EventDetail{
			ID:        "00000000-0000-4000-8000-000000000001",
			Type:      "order.confirmed",
			CreatedAt: time.Now().UTC(),
			Payload:   json.RawMessage(`{}`),
			Deliveries: []store.DeliveryDetail{
				{
					ID:            "00000000-0000-4000-8000-000000000011",
					OperationID:   "00000000-0000-4000-8000-000000000011",
					Status:        "pending",
					NextAttemptAt: &nextAttemptAt,
					MaxAttempts:   5,
					Attempts:      []store.AttemptDetail{},
					Replays:       []store.ReplayDetail{},
				},
			},
		},
	}
	handler := NewHandler(testSettings(t), repository)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, "/v1/events/00000000-0000-4000-8000-000000000001"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}

	var raw map[string]json.RawMessage
	_ = json.Unmarshal(recorder.Body.Bytes(), &raw)
	var deliveries []map[string]json.RawMessage
	_ = json.Unmarshal(raw["deliveries"], &deliveries)

	var nextAttemptAtVal *time.Time
	_ = json.Unmarshal(deliveries[0]["next_attempt_at"], &nextAttemptAtVal)
	if nextAttemptAtVal == nil || !nextAttemptAtVal.Equal(nextAttemptAt) {
		t.Fatalf("next_attempt_at = %v, want %v", nextAttemptAtVal, nextAttemptAt)
	}

	// confirmed_at and terminal_reason should be null for pending delivery.
	if string(deliveries[0]["confirmed_at"]) != "null" {
		t.Fatalf("confirmed_at = %s, want null", string(deliveries[0]["confirmed_at"]))
	}
	if string(deliveries[0]["terminal_reason"]) != "null" {
		t.Fatalf("terminal_reason = %s, want null", string(deliveries[0]["terminal_reason"]))
	}
	// lease_until should be null for pending.
	if string(deliveries[0]["lease_until"]) != "null" {
		t.Fatalf("lease_until = %s, want null for pending", string(deliveries[0]["lease_until"]))
	}
}

func TestGetEventDetailReturns404ForNotFound(t *testing.T) {
	repository := &testRepository{getEventError: store.ErrNotFound}
	handler := NewHandler(testSettings(t), repository)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, "/v1/events/00000000-0000-4000-8000-000000000001"))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("not-found status = %d, want 404", recorder.Code)
	}
	var resp errorEnvelope
	_ = json.Unmarshal(recorder.Body.Bytes(), &resp)
	if resp.Error.Code != codeNotFound {
		t.Fatalf("error code = %q, want not_found", resp.Error.Code)
	}
}

func TestGetEventDetailReturns400ForMalformedID(t *testing.T) {
	handler := NewHandler(testSettings(t), &testRepository{getEventError: store.ErrStoreUnavailable})
	for _, id := range []string{"not-a-uuid", "123", "", "00000000-0000-4000-8000-00000000000G", "-0000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-00000000000-"} {
		path := "/v1/events/" + id
		if id == "" {
			// Empty segment after prefix — handled as routing: /v1/events/ → invalid UUID param
			path = "/v1/events/"
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, path))
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("path=%q status = %d, want 400", path, recorder.Code)
		}
	}
}

func TestGetEventDetailRequiresAuth(t *testing.T) {
	handler := NewHandler(testSettings(t), &testRepository{})
	r := httptest.NewRequest(http.MethodGet, "/v1/events/00000000-0000-4000-8000-000000000001", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, r)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated detail = %d, want 401", recorder.Code)
	}
}

func TestGetEventDetailMethodNotAllowedForNonGET(t *testing.T) {
	handler := NewHandler(testSettings(t), &testRepository{})
	for _, method := range []string{http.MethodPost, http.MethodDelete, http.MethodPut} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, authenticatedRequest(method, "/v1/events/00000000-0000-4000-8000-000000000001"))
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /v1/events/id = %d, want 405", method, recorder.Code)
		}
	}
}

func TestGetEventDetailDoesNotExposeSensitiveFields(t *testing.T) {
	repository := &testRepository{
		getEventDetail: store.EventDetail{
			ID:         "00000000-0000-4000-8000-000000000001",
			Type:       "order.confirmed",
			CreatedAt:  time.Now().UTC(),
			Payload:    json.RawMessage(`{"order_id":"ord-1"}`),
			Deliveries: []store.DeliveryDetail{},
		},
	}
	handler := NewHandler(testSettings(t), repository)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authenticatedRequest(http.MethodGet, "/v1/events/00000000-0000-4000-8000-000000000001"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	for _, banned := range []string{"key_ref", "url_snapshot", "request_body", "request_hash", "wire_body", "token", "secret"} {
		if strings.Contains(body, banned) {
			t.Errorf("response body contained banned field %q: %s", banned, body)
		}
	}
}

// --- Cursor encoding round-trip ---

func TestCursorEncodeDecodeRoundTrip(t *testing.T) {
	c := store.EventCursor{
		CreatedAt: time.Date(2026, 11, 6, 8, 10, 11, 123000000, time.UTC),
		ID:        "00000000-0000-4000-8000-000000000001",
	}
	encoded, err := store.EncodeCursor(c)
	if err != nil {
		t.Fatal("encode:", err)
	}
	decoded, err := store.DecodeCursor(encoded)
	if err != nil {
		t.Fatal("decode:", err)
	}
	if decoded.ID != c.ID || !decoded.CreatedAt.Equal(c.CreatedAt) {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", decoded, c)
	}
}

func TestCursorDecodationRejectsMalformed(t *testing.T) {
	for _, bad := range []string{
		"",                       // empty
		"!!!",                    // invalid base64
		"e30K",                   // {} valid base64 but missing id/created_at
		strings.Repeat("a", 513), // too long
		"eyJjcmVhdGVkX2F0IjoiMjAyNi0xMS0wNlQwODoxMDoxMVoiLCJpZCI6Im5vdC11dWlkIn0", // non-UUID id
	} {
		_, err := store.DecodeCursor(bad)
		if err == nil {
			t.Errorf("DecodeCursor(%q) succeeded, want error", bad)
		}
	}
}
