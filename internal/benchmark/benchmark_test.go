package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

const benchmarkTestToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestBuildIngestBodyUsesExactWholeRequestSize(t *testing.T) {
	const target = 2048
	body, err := BuildIngestBody("bench_run_1", "order-00001", target)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != target {
		t.Fatalf("body size = %d, want %d", len(body), target)
	}
	if _, err = store.ValidateEventRequest(body); err != nil {
		t.Fatalf("padded request is invalid: %v", err)
	}
	if !bytes.Contains(body, []byte(`"padding":"`)) {
		t.Fatal("padded request is missing payload.padding")
	}
}

func TestBuildIngestBodyExplainsMinimumSize(t *testing.T) {
	_, err := BuildIngestBody("bench_run_1", "order-00001", 1)
	if err == nil || !strings.Contains(err.Error(), "minimum serialized request size") {
		t.Fatalf("minimum-size error = %v", err)
	}
}

func TestNearestRankPercentilesAndEmptySamples(t *testing.T) {
	distribution := summarize([]float64{10, 2, 1, 3, 9, 4, 8, 5, 7, 6})
	if distribution.Count != 10 || distribution.Min == nil || *distribution.Min != 1 || distribution.P50 == nil || *distribution.P50 != 5 || distribution.P95 == nil || *distribution.P95 != 10 || distribution.P99 == nil || *distribution.P99 != 10 || distribution.Max == nil || *distribution.Max != 10 {
		t.Fatalf("unexpected nearest-rank distribution: %+v", distribution)
	}
	if empty := summarize(nil); empty.Count != 0 || empty.P50 != nil || empty.P95 != nil || empty.P99 != nil {
		t.Fatalf("empty distribution = %+v", empty)
	}
}

func TestValidateOptionsEnforcesHardCaps(t *testing.T) {
	base := Options{Events: 1, ProducerConcurrency: 1, PayloadBytes: 1, Timeout: time.Second}
	cases := []Options{
		{Events: 0, ProducerConcurrency: 1, PayloadBytes: 1, Timeout: time.Second},
		{Events: MaxEvents + 1, ProducerConcurrency: 1, PayloadBytes: 1, Timeout: time.Second},
		{Events: 1, ProducerConcurrency: MaxConcurrency + 1, PayloadBytes: 1, Timeout: time.Second},
		{Events: 1, ProducerConcurrency: 1, PayloadBytes: MaxPayloadBytes + 1, Timeout: time.Second},
		{Events: 1, ProducerConcurrency: 1, PayloadBytes: 1, Timeout: MaxTimeout + time.Second},
		{Events: 1, ProducerConcurrency: 1, PayloadBytes: 1, Timeout: time.Second, RunID: "../unsafe"},
	}
	for index, options := range cases {
		if err := ValidateOptions(options); err == nil {
			t.Fatalf("case %d accepted invalid options %+v", index, options)
		}
	}
	if err := ValidateOptions(base); err != nil {
		t.Fatalf("valid small options rejected: %v", err)
	}
}

func TestIngestSendsOneRequestAndRecordsSanitizedOutcome(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+benchmarkTestToken {
			t.Error("benchmark request missed API authentication")
		}
		if r.Header.Get("Idempotency-Key") != "bench:run:nonce:00001" {
			t.Error("benchmark request key is not unique and run-scoped")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) != 1024 {
			t.Errorf("request body size = %d, error = %v", len(body), err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprint(w, `{"event_id":"00000000-0000-4000-8000-000000000101","deduplicated":false,"created_at":"2026-11-12T00:00:00Z","delivery_ids":["00000000-0000-4000-8000-000000000102","00000000-0000-4000-8000-000000000103"]}`)
	}))
	defer server.Close()
	runner := testRunner(server.URL)
	sample := runner.sendIngest(context.Background(), "run", "nonce", "bench_run_nonce", 1)
	if sample.Outcome != "accepted" || sample.EventID == "" || sample.LatencyNS <= 0 || sample.RequestBytes != 1024 {
		t.Fatalf("unexpected response sample: %+v", sample)
	}
	if calls.Load() != 1 {
		t.Fatalf("client sent %d requests; benchmark must not silently retry", calls.Load())
	}
}

func TestIngestRecords429And5xxWithoutRetry(t *testing.T) {
	for _, test := range []struct {
		status int
		class  string
	}{{http.StatusTooManyRequests, "http_429"}, {http.StatusBadGateway, "http_5xx"}} {
		t.Run(test.class, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(test.status)
				fmt.Fprint(w, benchmarkTestToken)
			}))
			defer server.Close()
			sample := testRunner(server.URL).sendIngest(context.Background(), "run", "nonce", "bench_run_nonce", 1)
			if sample.ErrorClass != test.class || sample.Attempted != true {
				t.Fatalf("unexpected failure sample: %+v", sample)
			}
			if calls.Load() != 1 {
				t.Fatalf("failed request was retried %d times", calls.Load()-1)
			}
			if strings.Contains(sample.ErrorClass, benchmarkTestToken) {
				t.Fatal("failure classification included a response body")
			}
		})
	}
}

func TestNewRunnerRejectsRemotePostgreSQL(t *testing.T) {
	settings := testSettings(t, "postgres://user:pass@db.example.invalid:5432/webhook?sslmode=disable")
	if _, err := NewRunner(settings, Endpoints{API: "http://api:8080", Controls: map[string]string{"fulfillment": "http://fulfillment:9091", "notification": "http://notification:9091"}}, Options{Events: 1, ProducerConcurrency: 1, PayloadBytes: 1024, Timeout: time.Second}, t.TempDir(), "", ""); err == nil || !strings.Contains(err.Error(), "local PostgreSQL") {
		t.Fatalf("runner accepted remote database: %v", err)
	}
}

func TestReconciliationMeasuresDurableCountsAndDetectsMismatch(t *testing.T) {
	created := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	firstAttempt := created.Add(50 * time.Millisecond)
	confirmed := created.Add(125 * time.Millisecond)
	event := eventRow{ID: "00000000-0000-4000-8000-000000000101", IdempotencyKey: "bench:run:nonce:00001", CreatedAt: created, RequestBytes: 1024, WireBytes: 1210}
	deliveries := []deliveryRow{
		{ID: "00000000-0000-4000-8000-000000000102", EventID: event.ID, Destination: "fulfillment", Status: "succeeded", CreatedAt: created, FirstStart: &firstAttempt, ConfirmedAt: &confirmed, Attempts: 1},
		{ID: "00000000-0000-4000-8000-000000000103", EventID: event.ID, Destination: "notification", Status: "succeeded", CreatedAt: created, FirstStart: &firstAttempt, ConfirmedAt: &confirmed, Attempts: 1},
	}
	snap := snapshot{
		Events:     []eventRow{event},
		Deliveries: deliveries,
		Receivers: map[string]receiverRow{
			"fulfillment":  {Code: "fulfillment", Mode: "normal", Dedupe: true, Requests: 1, RequestOperationCount: 1, RequestOps: []string{deliveries[0].ID}, Effects: 1, EffectOperationCount: 1, EffectOps: []string{deliveries[0].ID}},
			"notification": {Code: "notification", Mode: "normal", Dedupe: true, Requests: 1, RequestOperationCount: 1, RequestOps: []string{deliveries[1].ID}, Effects: 1, EffectOperationCount: 1, EffectOps: []string{deliveries[1].ID}},
		},
	}
	samples := []requestSample{{Sequence: 1, Attempted: true, Outcome: "accepted", EventID: event.ID, DeliveryIDs: []string{deliveries[0].ID, deliveries[1].ID}, RequestBytes: 1024, LatencyNS: int64(10 * time.Millisecond)}}
	keys := []string{event.IdempotencyKey}
	sequences := map[string]int{event.IdempotencyKey: 1}
	report := Report{Counts: CountsReport{PlannedRequests: 1, AttemptedRequests: 1, SuccessfulResponses: 1}}
	reconcileReport(&report, snap, samples, keys, sequences)
	if !report.Reconciliation.Passed || report.Reconciliation.ExpectedDeliveries != 2 {
		t.Fatalf("valid durable snapshot did not reconcile: %+v", report.Reconciliation)
	}
	populateCountsAndPerformance(&report, samples, snap, time.Second)
	if report.Counts.AcceptedUniqueEvents != 1 || report.Counts.Deliveries != 2 || report.Counts.PersistedAttempts != 2 || report.Counts.ReceiverRequests != 2 || report.Counts.ReceiverEffects != 2 {
		t.Fatalf("durable counts = %+v", report.Counts)
	}
	if report.Performance.AcceptedEventsPerSecond != 1 || report.Performance.RetryAmplification == nil || *report.Performance.RetryAmplification != 1 {
		t.Fatalf("performance ratios = %+v", report.Performance)
	}

	samples[0].EventID = "00000000-0000-4000-8000-000000000199"
	reconcileReport(&report, snap, samples, keys, sequences)
	if report.Reconciliation.Passed || !contains(report.Reconciliation.Issues, "api_event_id_mismatch") {
		t.Fatalf("mismatched API event ID did not fail reconciliation: %+v", report.Reconciliation)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func testRunner(apiURL string) *Runner {
	settings := testSettings(nil, "postgres://user:pass@127.0.0.1:5432/webhook_dev?sslmode=disable")
	return &Runner{
		settings:  settings,
		endpoints: Endpoints{API: apiURL, Controls: map[string]string{"fulfillment": "http://127.0.0.1:9092", "notification": "http://127.0.0.1:9093"}},
		options:   Options{Events: 1, ProducerConcurrency: 1, PayloadBytes: 1024, Timeout: time.Second},
		client:    &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}},
	}
}

func testSettings(t testing.TB, databaseURL string) config.Settings {
	values := map[string]string{
		"DATABASE_URL": databaseURL, "RUN_MODE": "host", "API_TOKEN": benchmarkTestToken,
		"DEMO_CONTROL_TOKEN": benchmarkTestToken,
	}
	settings, err := config.LoadFrom(config.ServiceBenchmark, func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		panic(err)
	}
	return settings
}
