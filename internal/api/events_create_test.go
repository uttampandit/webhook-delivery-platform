package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

const validCreateEventBody = `{"type":"order.confirmed","payload":{"scenario_id":"api_test","order_id":"order_test","quantity":1},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`

func TestCreateEventMapsRepositoryResultsAndUsesAuthenticatedTenant(t *testing.T) {
	createdAt := time.Date(2026, 11, 6, 8, 10, 11, 123000000, time.UTC)
	repository := &testRepository{acceptedEvent: store.AcceptedEvent{
		EventID: "00000000-0000-4000-8000-000000000301", CreatedAt: createdAt,
		DeliveryIDs: []string{"00000000-0000-4000-8000-000000000302"},
	}}
	handler := NewHandler(testSettings(t), repository)

	request := newCreateEventRequest([]byte(validCreateEventBody), "order-key-1", true)
	request.Header.Set("X-Tenant-ID", "00000000-0000-4000-8000-000000000099")
	request.Header.Set("X-Actor", "caller-selected-actor")
	acceptedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(acceptedRecorder, request)
	if acceptedRecorder.Code != http.StatusAccepted {
		t.Fatalf("new event status = %d body=%s, want 202", acceptedRecorder.Code, acceptedRecorder.Body.String())
	}
	var accepted struct {
		EventID      string    `json:"event_id"`
		Deduplicated bool      `json:"deduplicated"`
		CreatedAt    time.Time `json:"created_at"`
		DeliveryIDs  []string  `json:"delivery_ids"`
	}
	if err := json.Unmarshal(acceptedRecorder.Body.Bytes(), &accepted); err != nil {
		t.Fatal("decode accepted response:", err)
	}
	if accepted.EventID != repository.acceptedEvent.EventID || accepted.Deduplicated || !accepted.CreatedAt.Equal(createdAt) ||
		len(accepted.DeliveryIDs) != 1 || accepted.DeliveryIDs[0] != repository.acceptedEvent.DeliveryIDs[0] {
		t.Fatalf("accepted response = %+v", accepted)
	}
	if repository.acceptCalls != 1 || repository.acceptTenantID != config.TenantID || repository.acceptActor != operatorActor ||
		repository.acceptKey != "order-key-1" || !repository.acceptDeadline {
		t.Fatalf("repository call used unexpected identity or context: %+v", repository)
	}

	repository.acceptedEvent.Deduplicated = true
	duplicateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(duplicateRecorder, newCreateEventRequest([]byte(validCreateEventBody), "order-key-1", true))
	if duplicateRecorder.Code != http.StatusOK {
		t.Fatalf("duplicate event status = %d body=%s, want 200", duplicateRecorder.Code, duplicateRecorder.Body.String())
	}
	if err := json.Unmarshal(duplicateRecorder.Body.Bytes(), &accepted); err != nil {
		t.Fatal("decode duplicate response:", err)
	}
	if !accepted.Deduplicated || accepted.EventID != repository.acceptedEvent.EventID || accepted.DeliveryIDs[0] != repository.acceptedEvent.DeliveryIDs[0] {
		t.Fatalf("duplicate response changed acceptance identity: %+v", accepted)
	}
}

func TestCreateEventValidationErrorsAndRepositoryMapping(t *testing.T) {
	depthExceeded := `{"type":"order.confirmed","payload":{"x":` + strings.Repeat("[", 32) + `0` + strings.Repeat("]", 32) + `},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`
	invalidUTF8 := append([]byte(`{"type":"order.confirmed","payload":{"value":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`)...)
	for _, test := range []struct {
		name       string
		body       []byte
		key        string
		duplicate  bool
		repository error
		status     int
		code       errorCode
	}{
		{name: "missing key", body: []byte(validCreateEventBody), status: http.StatusBadRequest, code: codeInvalidRequest},
		{name: "invalid key", body: []byte(validCreateEventBody), key: "has space", status: http.StatusBadRequest, code: codeInvalidRequest},
		{name: "duplicate key headers", body: []byte(validCreateEventBody), key: "key", duplicate: true, status: http.StatusBadRequest, code: codeInvalidRequest},
		{name: "malformed JSON", body: []byte(`{"type":`), key: "key", status: http.StatusBadRequest, code: codeInvalidJSON},
		{name: "duplicate JSON key", body: []byte(`{"type":"order.confirmed","\u0074ype":"other","payload":{},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`), key: "key", status: http.StatusBadRequest, code: codeInvalidJSON},
		{name: "trailing JSON", body: []byte(validCreateEventBody + ` {}`), key: "key", status: http.StatusBadRequest, code: codeInvalidJSON},
		{name: "malformed UTF-8", body: invalidUTF8, key: "key", status: http.StatusBadRequest, code: codeInvalidJSON},
		{name: "nesting over limit", body: []byte(depthExceeded), key: "key", status: http.StatusBadRequest, code: codeInvalidJSON},
		{name: "unknown top-level field", body: []byte(strings.TrimSuffix(validCreateEventBody, "}") + `,"other":true}`), key: "key", status: http.StatusBadRequest, code: codeInvalidRequest},
		{name: "null payload", body: []byte(`{"type":"order.confirmed","payload":null,"destination_ids":["00000000-0000-4000-8000-000000000101"]}`), key: "key", status: http.StatusBadRequest, code: codeInvalidRequest},
		{name: "repeated destination", body: []byte(`{"type":"order.confirmed","payload":{},"destination_ids":["00000000-0000-4000-8000-000000000101","00000000-0000-4000-8000-000000000101"]}`), key: "key", status: http.StatusBadRequest, code: codeInvalidRequest},
		{name: "destination outside fixed registry", body: []byte(`{"type":"order.confirmed","payload":{},"destination_ids":["00000000-0000-4000-8000-000000000199"]}`), key: "key", status: http.StatusBadRequest, code: codeUnknownDestination},
		{name: "unknown destination", body: []byte(validCreateEventBody), key: "key", repository: store.ErrUnknownDestination, status: http.StatusBadRequest, code: codeUnknownDestination},
		{name: "idempotency conflict", body: []byte(validCreateEventBody), key: "key", repository: store.ErrIdempotencyConflict, status: http.StatusConflict, code: codeIdempotencyConflict},
		{name: "ambiguous commit", body: []byte(validCreateEventBody), key: "key", repository: store.ErrAmbiguousCommit, status: http.StatusServiceUnavailable, code: codeDependencyUnavailable},
		{name: "store unavailable", body: []byte(validCreateEventBody), key: "key", repository: store.ErrStoreUnavailable, status: http.StatusServiceUnavailable, code: codeDependencyUnavailable},
		{name: "wire body too large", body: []byte(validCreateEventBody), key: "key", repository: store.ErrOutgoingBodyTooLarge, status: http.StatusRequestEntityTooLarge, code: codeBodyTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := &testRepository{acceptError: test.repository}
			handler := NewHandler(testSettings(t), repository)
			request := newCreateEventRequest(test.body, test.key, true)
			if test.duplicate {
				request.Header.Add("Idempotency-Key", test.key)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Fatalf("status=%d body=%q, want %d", recorder.Code, recorder.Body.String(), test.status)
			}
			var response errorEnvelope
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal("decode error response:", err)
			}
			if response.Error.Code != test.code || response.Error.RequestID == "" || response.Error.RequestID != recorder.Header().Get("X-Request-Id") {
				t.Fatalf("error envelope=%+v request-id=%q", response.Error, recorder.Header().Get("X-Request-Id"))
			}
			if strings.Contains(recorder.Body.String(), "SQL") || strings.Contains(recorder.Body.String(), "rollback") {
				t.Fatalf("error response disclosed implementation details: %s", recorder.Body.String())
			}
			if test.repository == nil && repository.acceptCalls != 0 {
				t.Fatalf("invalid request reached repository %d times", repository.acceptCalls)
			}
			if test.repository != nil && repository.acceptCalls != 1 {
				t.Fatalf("valid request reached repository %d times, want 1", repository.acceptCalls)
			}
		})
	}
}

func TestCreateEventBodyLimitIgnoresContentLength(t *testing.T) {
	body := bytes.Repeat([]byte("x"), store.MaxEventRequestBytes+1)
	for _, contentLength := range []int64{-1, 1} {
		t.Run(map[int64]string{-1: "absent", 1: "understated"}[contentLength], func(t *testing.T) {
			repository := &testRepository{}
			handler := NewHandler(testSettings(t), repository)
			request := newCreateEventRequest(body, "large-body", true)
			request.ContentLength = contentLength
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			assertCreateEventError(t, recorder, http.StatusRequestEntityTooLarge, codeBodyTooLarge)
			if repository.acceptCalls != 0 {
				t.Fatalf("oversized request reached repository %d times", repository.acceptCalls)
			}
		})
	}
}

func TestCreateEventRequiresAuthenticationAndPOST(t *testing.T) {
	repository := &testRepository{}
	handler := NewHandler(testSettings(t), repository)

	unauthorized := newCreateEventRequest([]byte(validCreateEventBody), "key", false)
	unauthorizedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(unauthorizedRecorder, unauthorized)
	assertCreateEventError(t, unauthorizedRecorder, http.StatusUnauthorized, codeUnauthorized)
	if repository.acceptCalls != 0 {
		t.Fatal("unauthenticated event request reached repository")
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	get.Header.Set("Authorization", "Bearer "+testAPIToken)
	getRecorder := httptest.NewRecorder()
	handler.ServeHTTP(getRecorder, get)
	if getRecorder.Code != http.StatusMethodNotAllowed || getRecorder.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET /v1/events = %d Allow=%q, want 405 Allow: POST", getRecorder.Code, getRecorder.Header().Get("Allow"))
	}
}

func TestCreateEventParserFailuresDoNotLogRequestBody(t *testing.T) {
	const bodyMarker = "sensitive-payload-marker"
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	handler := NewHandler(testSettings(t), &testRepository{})
	body := []byte(`{"type":"order.confirmed","payload":{"private":"` + bodyMarker)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, newCreateEventRequest(body, "bad-json", true))
	assertCreateEventError(t, recorder, http.StatusBadRequest, codeInvalidJSON)
	if strings.Contains(logs.String(), bodyMarker) || strings.Contains(recorder.Body.String(), bodyMarker) {
		t.Fatal("parser failure disclosed request body")
	}
}

func newCreateEventRequest(body []byte, key string, authenticated bool) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	if authenticated {
		request.Header.Set("Authorization", "Bearer "+testAPIToken)
	}
	return request
}

func assertCreateEventError(t *testing.T, recorder *httptest.ResponseRecorder, status int, code errorCode) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status=%d body=%q, want %d", recorder.Code, recorder.Body.String(), status)
	}
	var response errorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal("decode error response:", err)
	}
	if response.Error.Code != code || response.Error.RequestID == "" || response.Error.RequestID != recorder.Header().Get("X-Request-Id") {
		t.Fatalf("error envelope=%+v request-id=%q", response.Error, recorder.Header().Get("X-Request-Id"))
	}
}
