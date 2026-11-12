package telemetry

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var histogramBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

type Metrics struct {
	registry                *prometheus.Registry
	ingestTotal             *prometheus.CounterVec
	ingestDuration          prometheus.Histogram
	attemptsTotal           *prometheus.CounterVec
	deliveriesTotal         *prometheus.CounterVec
	staleFinalizationTotal  prometheus.Counter
	queueDue                prometheus.Gauge
	inFlight                prometheus.Gauge
	dead                    prometheus.Gauge
	queueDelay              prometheus.Histogram
	deliveryLatency         prometheus.Histogram
	gaugeCollectionFailures prometheus.Counter
}

type GaugeSnapshot struct {
	Due      int64
	InFlight int64
	Dead     int64
}

type GaugeReadFunc func(context.Context) (GaugeSnapshot, error)

func NewMetrics() (*Metrics, error) {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		ingestTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "webhook_ingest_total",
			Help: "Event ingest requests grouped by their bounded result.",
		}, []string{"result"}),
		ingestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "webhook_ingest_duration_seconds",
			Help:    "Time spent handling event ingest POST requests.",
			Buckets: append([]float64(nil), histogramBuckets...),
		}),
		attemptsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "webhook_attempts_total",
			Help: "Attempt outcomes finalized or recovered by this process since its start.",
		}, []string{"outcome"}),
		deliveriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "webhook_deliveries_total",
			Help: "Terminal delivery outcomes observed by this process since its start.",
		}, []string{"result"}),
		staleFinalizationTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "webhook_stale_finalization_total",
			Help: "Finalization writes rejected by the delivery fencing checks.",
		}),
		queueDue: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "webhook_queue_due",
			Help: "Pending deliveries due for a claim in the configured tenant.",
		}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "webhook_in_flight",
			Help: "Deliveries currently in flight in the configured tenant.",
		}),
		dead: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "webhook_dead",
			Help: "Dead deliveries in the configured tenant.",
		}),
		queueDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "webhook_queue_delay_seconds",
			Help:    "Time from a delivery's scheduled due time to its committed claim.",
			Buckets: append([]float64(nil), histogramBuckets...),
		}),
		deliveryLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "webhook_delivery_latency_seconds",
			Help:    "Time from event creation to its first confirmed delivery; unsuccessful deliveries are excluded.",
			Buckets: append([]float64(nil), histogramBuckets...),
		}),
		gaugeCollectionFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "webhook_gauge_collection_failures_total",
			Help: "Timed out or failed database-backed gauge refreshes.",
		}),
	}
	collectors := []prometheus.Collector{
		metrics.ingestTotal,
		metrics.ingestDuration,
		metrics.attemptsTotal,
		metrics.deliveriesTotal,
		metrics.staleFinalizationTotal,
		metrics.queueDue,
		metrics.inFlight,
		metrics.dead,
		metrics.queueDelay,
		metrics.deliveryLatency,
		metrics.gaugeCollectionFailures,
	}
	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			return nil, err
		}
	}
	return metrics, nil
}

func HistogramBuckets() []float64 {
	return append([]float64(nil), histogramBuckets...)
}

func (m *Metrics) ObserveIngest(result string, duration time.Duration) {
	if m == nil {
		return
	}
	if _, ok := ingestResults[result]; !ok {
		result = "error"
	}
	m.ingestTotal.WithLabelValues(result).Inc()
	m.ingestDuration.Observe(nonNegativeSeconds(duration))
}

func (m *Metrics) ObserveAttempt(outcome string) {
	if m == nil {
		return
	}
	if _, ok := attemptOutcomes[outcome]; !ok {
		return
	}
	m.attemptsTotal.WithLabelValues(outcome).Inc()
}

func (m *Metrics) AddRecoveredAttempts(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.attemptsTotal.WithLabelValues("unconfirmed").Add(float64(count))
}

func (m *Metrics) ObserveDelivery(result string) {
	m.AddDeliveries(result, 1)
}

func (m *Metrics) AddDeliveries(result string, count int) {
	if m == nil || count <= 0 {
		return
	}
	if _, ok := deliveryResults[result]; !ok {
		return
	}
	m.deliveriesTotal.WithLabelValues(result).Add(float64(count))
}

func (m *Metrics) ObserveStaleFinalization() {
	if m != nil {
		m.staleFinalizationTotal.Inc()
	}
}

func (m *Metrics) ObserveQueueDelay(delay time.Duration) {
	if m != nil {
		m.queueDelay.Observe(nonNegativeSeconds(delay))
	}
}

func (m *Metrics) ObserveDeliveryLatency(latency time.Duration) {
	if m != nil {
		m.deliveryLatency.Observe(nonNegativeSeconds(latency))
	}
}

func (m *Metrics) SetQueueGauges(snapshot GaugeSnapshot) {
	if m == nil {
		return
	}
	m.queueDue.Set(nonNegative(snapshot.Due))
	m.inFlight.Set(nonNegative(snapshot.InFlight))
	m.dead.Set(nonNegative(snapshot.Dead))
}

func (m *Metrics) GaugeCollectionFailed() {
	if m != nil {
		m.gaugeCollectionFailures.Inc()
	}
}

func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) RunGaugeCollector(
	ctx context.Context,
	interval time.Duration,
	timeout time.Duration,
	read GaugeReadFunc,
) {
	if m == nil || read == nil || interval <= 0 || timeout <= 0 {
		return
	}
	collect := func() {
		work, cancel := context.WithTimeout(ctx, timeout)
		snapshot, err := read(work)
		cancel()
		if err != nil {
			m.GaugeCollectionFailed()
			return
		}
		m.SetQueueGauges(snapshot)
	}
	collect()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			collect()
		}
	}
}

func nonNegative(value int64) float64 {
	if value < 0 {
		return 0
	}
	return float64(value)
}

func nonNegativeSeconds(value time.Duration) float64 {
	if value < 0 {
		return 0
	}
	return value.Seconds()
}

var ingestResults = map[string]struct{}{
	"accepted": {}, "deduplicated": {}, "conflict": {}, "invalid": {}, "error": {},
}

var attemptOutcomes = map[string]struct{}{
	"acknowledged": {}, "http_failure": {}, "unconfirmed": {}, "local_failure": {},
}

var deliveryResults = map[string]struct{}{
	"succeeded": {}, "dead": {},
}
