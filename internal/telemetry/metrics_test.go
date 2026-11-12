package telemetry

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsUseFixedLabelsUnitsAndBuckets(t *testing.T) {
	metrics, err := NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	metrics.ObserveIngest("accepted", 25*time.Millisecond)
	metrics.ObserveIngest("planted-label", 40*time.Millisecond)
	metrics.ObserveAttempt("acknowledged")
	metrics.ObserveAttempt("planted-attempt-label")
	metrics.ObserveDelivery("succeeded")
	metrics.ObserveStaleFinalization()
	metrics.ObserveQueueDelay(100 * time.Millisecond)
	metrics.ObserveDeliveryLatency(250 * time.Millisecond)
	metrics.SetQueueGauges(GaugeSnapshot{Due: 3, InFlight: 2, Dead: 1})
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	got := recorder.Body.String()
	for _, expected := range []string{
		`webhook_ingest_total{result="accepted"} 1`,
		`webhook_ingest_total{result="error"} 1`,
		`webhook_attempts_total{outcome="acknowledged"} 1`,
		`webhook_stale_finalization_total 1`,
		`webhook_queue_due 3`,
		`webhook_in_flight 2`,
		`webhook_dead 1`,
		`webhook_queue_delay_seconds_bucket{le="0.1"} 1`,
		`webhook_delivery_latency_seconds_bucket{le="0.25"} 1`,
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("metrics omitted %q:\n%s", expected, got)
		}
	}
	for _, forbidden := range []string{"planted-label", "planted-attempt-label", "00000000-", "url=", "payload="} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("metrics exposed unbounded value %q:\n%s", forbidden, got)
		}
	}
	if len(HistogramBuckets()) != 13 || HistogramBuckets()[0] != 0.005 || HistogramBuckets()[12] != 60 {
		t.Fatalf("unexpected histogram buckets: %v", HistogramBuckets())
	}
}

func TestGaugeCollectorRetainsLastGoodSnapshotAndCountsFailure(t *testing.T) {
	metrics, err := NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	calls := 0
	go func() {
		defer close(done)
		metrics.RunGaugeCollector(ctx, time.Millisecond, time.Second, func(context.Context) (GaugeSnapshot, error) {
			calls++
			if calls == 1 {
				return GaugeSnapshot{Due: 7, InFlight: 4, Dead: 2}, nil
			}
			cancel()
			return GaugeSnapshot{}, context.DeadlineExceeded
		})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("gauge collector did not stop after cancellation")
	}
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	got := recorder.Body.String()
	for _, expected := range []string{"webhook_queue_due 7", "webhook_in_flight 4", "webhook_dead 2", "webhook_gauge_collection_failures_total 1"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("gauge failure changed prior values or missed failure counter %q:\n%s", expected, got)
		}
	}
}

func TestGaugeCollectorAppliesReadTimeout(t *testing.T) {
	metrics, err := NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		metrics.RunGaugeCollector(ctx, time.Hour, 5*time.Millisecond, func(work context.Context) (GaugeSnapshot, error) {
			<-work.Done()
			cancel()
			return GaugeSnapshot{}, work.Err()
		})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("gauge query was not bounded by its timeout")
	}
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(recorder.Body.String(), "webhook_gauge_collection_failures_total 1") {
		t.Fatalf("timed out gauge read was not counted: %s", recorder.Body.String())
	}
}
