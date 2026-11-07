package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

const testAPIToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type testRepository struct {
	pingError       error
	readinessError  error
	listError       error
	destinations    []store.DestinationSummary
	acceptError     error
	acceptedEvent   store.AcceptedEvent
	listEventsError error
	listEventsItems []store.EventListItem
	getEventError   error
	getEventDetail  store.EventDetail
	listedTenantID  string
	listedActor     string
	listCalls       int
	acceptTenantID  string
	acceptKey       string
	acceptActor     string
	acceptCalls     int
	acceptDeadline  bool
}

func (repository *testRepository) Ping(context.Context) error { return repository.pingError }

func (repository *testRepository) CheckReadiness(context.Context, config.Registry) error {
	return repository.readinessError
}

func (repository *testRepository) ListDestinations(ctx context.Context, tenantID string) ([]store.DestinationSummary, error) {
	repository.listCalls++
	repository.listedTenantID = tenantID
	if principal, ok := PrincipalFromContext(ctx); ok {
		repository.listedActor = principal.Actor
	}
	return repository.destinations, repository.listError
}

func (repository *testRepository) AcceptEvent(ctx context.Context, tenantID, key string, _ store.ValidatedEventRequest) (store.AcceptedEvent, error) {
	repository.acceptCalls++
	repository.acceptTenantID = tenantID
	repository.acceptKey = key
	_, repository.acceptDeadline = ctx.Deadline()
	if principal, ok := PrincipalFromContext(ctx); ok {
		repository.acceptActor = principal.Actor
	}
	return repository.acceptedEvent, repository.acceptError
}

func (repository *testRepository) ListEvents(_ context.Context, _ string, _ int, _ *store.EventCursor) ([]store.EventListItem, *store.EventCursor, error) {
	if repository.listEventsError != nil {
		return nil, nil, repository.listEventsError
	}
	items := repository.listEventsItems
	if items == nil {
		items = []store.EventListItem{}
	}
	return items, nil, nil
}

func (repository *testRepository) GetEvent(_ context.Context, _, _ string) (store.EventDetail, error) {
	return repository.getEventDetail, repository.getEventError
}

func TestBearerAuthenticationAndRequestIDEnvelope(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	repository := &testRepository{destinations: []store.DestinationSummary{{ID: config.FulfillmentDestination, Code: "fulfillment"}}}
	handler := NewHandler(testSettings(t), repository)
	const plantedToken = "planted-incorrect-token-that-must-not-be-logged"
	cases := []struct {
		name   string
		header string
	}{
		{name: "missing", header: ""},
		{name: "wrong", header: "Bearer " + plantedToken},
		{name: "malformed scheme", header: "Basic " + testAPIToken},
		{name: "missing token", header: "Bearer"},
		{name: "extra field", header: "Bearer " + testAPIToken + " extra"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/v1/destinations", nil)
			request.Header.Set("Authorization", test.header)
			request.Header.Set("X-Request-Id", strings.Repeat("caller-controlled-", 1024))
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", recorder.Code)
			}
			var response errorEnvelope
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal("decode auth error envelope:", err)
			}
			requestID := recorder.Header().Get("X-Request-Id")
			if response.Error.Code != codeUnauthorized || response.Error.Message != "unauthorized" {
				t.Fatalf("auth error = %+v", response.Error)
			}
			if requestID == "" || requestID != response.Error.RequestID || requestID == request.Header.Get("X-Request-Id") {
				t.Fatalf("request ID header=%q envelope=%q", requestID, response.Error.RequestID)
			}
			if strings.Contains(recorder.Body.String(), plantedToken) {
				t.Fatal("auth error disclosed supplied token")
			}
		})
	}
	if repository.listCalls != 0 {
		t.Fatalf("unauthorized requests reached repository %d times", repository.listCalls)
	}
	if !strings.Contains(logs.String(), "request_id=") {
		t.Fatal("request logs did not include server request IDs")
	}
	if strings.Contains(logs.String(), plantedToken) || strings.Contains(logs.String(), "Authorization") {
		t.Fatal("request logs disclosed authorization material")
	}
}

func TestAuthenticatedDestinationsIgnoreCallerIdentityAndLimitFields(t *testing.T) {
	repository := &testRepository{destinations: []store.DestinationSummary{
		{ID: config.FulfillmentDestination, Code: "fulfillment"},
		{ID: config.NotificationDestination, Code: "notification"},
	}}
	handler := NewHandler(testSettings(t), repository)
	request := httptest.NewRequest(http.MethodGet, "/v1/destinations", nil)
	request.Header.Set("Authorization", "Bearer "+testAPIToken)
	request.Header.Set("X-Tenant-ID", "00000000-0000-4000-8000-000000000099")
	request.Header.Set("X-Actor", "attacker-selected-actor")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if repository.listedTenantID != config.TenantID {
		t.Fatalf("repository tenant = %q, want configured tenant %q", repository.listedTenantID, config.TenantID)
	}
	if repository.listedActor != operatorActor {
		t.Fatalf("repository actor = %q, want configured actor %q", repository.listedActor, operatorActor)
	}
	var response struct {
		Items []map[string]json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal("decode destinations response:", err)
	}
	if len(response.Items) != 2 {
		t.Fatalf("destination count = %d, want 2", len(response.Items))
	}
	for _, item := range response.Items {
		if len(item) != 2 || item["id"] == nil || item["code"] == nil {
			t.Fatalf("destination response exposed unexpected fields: %+v", item)
		}
	}
	if recorder.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("handler enabled CORS")
	}
}

func TestDuplicateAuthorizationHeadersAreRejected(t *testing.T) {
	repository := &testRepository{}
	handler := NewHandler(testSettings(t), repository)
	request := httptest.NewRequest(http.MethodGet, "/v1/destinations", nil)
	request.Header.Add("Authorization", "Bearer "+testAPIToken)
	request.Header.Add("Authorization", "Bearer another-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("duplicate authorization status = %d, want 401", recorder.Code)
	}
	if repository.listCalls != 0 {
		t.Fatal("duplicate authorization headers reached the repository")
	}
}

func TestHealthRoutesAndReadinessFailuresAreGeneric(t *testing.T) {
	const planted = "postgres://planted-user:planted-password@private-db/private-schema"
	repository := &testRepository{}
	handler := NewHandler(testSettings(t), repository)

	liveRecorder := serve(handler, http.MethodGet, "/health/live", "")
	if liveRecorder.Code != http.StatusOK || strings.TrimSpace(liveRecorder.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("live response = %d %q", liveRecorder.Code, liveRecorder.Body.String())
	}

	readyRecorder := serve(handler, http.MethodGet, "/health/ready", "")
	if readyRecorder.Code != http.StatusOK || strings.TrimSpace(readyRecorder.Body.String()) != `{"status":"ready"}` {
		t.Fatalf("ready response = %d %q", readyRecorder.Code, readyRecorder.Body.String())
	}

	repository.pingError = errors.New(planted)
	readyRecorder = serve(handler, http.MethodGet, "/health/ready", "")
	if readyRecorder.Code != http.StatusServiceUnavailable || strings.TrimSpace(readyRecorder.Body.String()) != `{"status":"not_ready"}` {
		t.Fatalf("DB failure response = %d %q", readyRecorder.Code, readyRecorder.Body.String())
	}
	if strings.Contains(readyRecorder.Body.String(), planted) {
		t.Fatal("readiness response disclosed database details")
	}

	repository.pingError = nil
	repository.readinessError = errors.New("private schema or SQL detail")
	readyRecorder = serve(handler, http.MethodGet, "/health/ready", "")
	if readyRecorder.Code != http.StatusServiceUnavailable || strings.Contains(readyRecorder.Body.String(), "private schema") {
		t.Fatalf("schema failure was not redacted: %d %q", readyRecorder.Code, readyRecorder.Body.String())
	}
}

func TestV1FallbackIsAuthenticatedAndMethodsSetAllow(t *testing.T) {
	handler := NewHandler(testSettings(t), &testRepository{})
	unauthorized := serve(handler, http.MethodGet, "/v1/events", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unknown unauthenticated /v1 route status = %d, want 401", unauthorized.Code)
	}
	// GET /v1/events is now valid; check DELETE returns 405 with correct Allow.
	deleteEvents := serve(handler, http.MethodDelete, "/v1/events", "Bearer "+testAPIToken)
	if deleteEvents.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /v1/events response = %d, want 405", deleteEvents.Code)
	}
	// GET /v1/events with valid auth should return 200 (empty list).
	listEvents := serve(handler, http.MethodGet, "/v1/events", "Bearer "+testAPIToken)
	if listEvents.Code != http.StatusOK {
		t.Fatalf("GET /v1/events response = %d body=%s, want 200", listEvents.Code, listEvents.Body.String())
	}

	for _, path := range []string{"/health/live", "/v1/destinations"} {
		header := ""
		if strings.HasPrefix(path, "/v1/") {
			header = "Bearer " + testAPIToken
		}
		recorder := serve(handler, http.MethodPost, path, header)
		if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodGet {
			t.Errorf("%s POST response = %d, Allow=%q", path, recorder.Code, recorder.Header().Get("Allow"))
		}
	}
}

func testSettings(t *testing.T) config.Settings {
	t.Helper()
	values := map[string]string{
		"DATABASE_URL": "postgres://user:password@127.0.0.1:5432/webhook_dev?sslmode=disable",
		"RUN_MODE":     "compose",
		"API_TOKEN":    testAPIToken,
	}
	settings, err := config.LoadFrom(config.ServiceAPI, func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatal("load API test settings:", err)
	}
	return settings
}

func serve(handler http.Handler, method, path, authorization string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, nil)
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestRequestIDsAreUUIDv4(t *testing.T) {
	id, err := newRequestID()
	if err != nil {
		t.Fatal("generate request ID:", err)
	}
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' || id[14] != '4' {
		t.Fatalf("request ID %q is not a UUIDv4", id)
	}
	if _, err := hex.DecodeString(strings.ReplaceAll(id, "-", "")); err != nil {
		t.Fatalf("request ID %q contains non-hex digits: %v", id, err)
	}
}
