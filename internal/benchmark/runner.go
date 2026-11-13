package benchmark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

const (
	benchmarkDatabasePoolMax = 2
	maxResponseBytes         = 1 << 20
	maxRunIDLength           = 40
)

type Runner struct {
	settings   config.Settings
	endpoints  Endpoints
	options    Options
	outputRoot string
	command    []string
	host       HostMetadata
	runtime    RuntimeMetadata
	client     *http.Client
}

func NewRunner(settings config.Settings, endpoints Endpoints, options Options, outputRoot string, hostJSON, runtimeJSON string) (*Runner, error) {
	if err := ValidateOptions(options); err != nil {
		return nil, err
	}
	if err := localDatabase(settings.DatabaseURL(), settings.RunMode()); err != nil {
		return nil, err
	}
	for _, secret := range []string{settings.APIToken(), settings.DemoControlToken()} {
		for _, argument := range options.Command {
			if secret != "" && strings.Contains(argument, secret) {
				return nil, errors.New("benchmark command arguments must not contain credentials")
			}
		}
	}
	if endpoints.API == "" || endpoints.Controls["fulfillment"] == "" || endpoints.Controls["notification"] == "" {
		return nil, errors.New("benchmark local endpoints are incomplete")
	}
	if outputRoot == "" {
		outputRoot = filepath.Join("artifacts", "bench")
	}
	var host HostMetadata
	if hostJSON != "" {
		if err := json.Unmarshal([]byte(hostJSON), &host); err != nil {
			return nil, errors.New("benchmark host metadata is invalid")
		}
	}
	var runtimeInfo RuntimeMetadata
	if runtimeJSON != "" {
		if err := json.Unmarshal([]byte(runtimeJSON), &runtimeInfo); err != nil {
			return nil, errors.New("benchmark runtime metadata is invalid")
		}
	}
	if err := validateRuntimeMetadata(runtimeInfo); err != nil {
		return nil, err
	}
	if runtimeInfo.Services == nil {
		runtimeInfo.Services = map[string]ContainerMetadata{}
	}
	if runtimeInfo.Worker.Concurrency < 1 {
		runtimeInfo.Worker = WorkerConfiguration{
			Concurrency:        settings.WorkerConcurrency(),
			PollInterval:       settings.WorkerPollInterval().String(),
			LeaseDuration:      settings.LeaseDuration().String(),
			LeaseMargin:        settings.LeaseMargin().String(),
			HTTPTimeout:        settings.HTTPTimeout().String(),
			DBStatementTimeout: settings.DBStatementTimeout().String(),
			DatabasePoolMax:    settings.WorkerConcurrency() + 2,
		}
	}
	if runtimeInfo.Worker.DatabasePoolMax == 0 {
		runtimeInfo.Worker.DatabasePoolMax = runtimeInfo.Worker.Concurrency + 2
	}
	if len(settings.APIToken()) == 0 || len(settings.DemoControlToken()) == 0 {
		return nil, errors.New("benchmark API and receiver control tokens are required")
	}
	transport := &http.Transport{
		Proxy:                  nil,
		DisableKeepAlives:      true,
		DisableCompression:     true,
		MaxResponseHeaderBytes: 16 * 1024,
	}
	return &Runner{
		settings: settings, endpoints: endpoints, options: options, outputRoot: outputRoot,
		command: append([]string(nil), options.Command...), host: host, runtime: runtimeInfo,
		client: &http.Client{
			Transport: transport,
			Timeout:   settings.HTTPTimeout(),
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func localDatabase(raw string, mode config.RunMode) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return errors.New("benchmark database URL is invalid")
	}
	host := strings.ToLower(parsed.Hostname())
	local := host == "localhost" || host == "127.0.0.1" || host == "::1"
	if mode == config.RunModeCompose && host == "postgres" {
		local = true
	}
	if !local {
		return errors.New("benchmark database must be the local PostgreSQL service")
	}
	return nil
}

func validateRuntimeMetadata(value RuntimeMetadata) error {
	allowed := map[string]bool{"api": true, "worker": true, "fulfillment": true, "notification": true, "postgres": true, "benchmark": true}
	for name, metadata := range value.Services {
		if !allowed[name] || len(metadata.ImageRef) > 256 || len(metadata.ImageID) > 256 || len(metadata.CPUSet) > 128 {
			return errors.New("benchmark runtime metadata is invalid")
		}
	}
	w := value.Worker
	if w.Concurrency < 0 || w.Concurrency > MaxConcurrency || w.DatabasePoolMax < 0 || w.DatabasePoolMax > 64 {
		return errors.New("benchmark worker metadata is invalid")
	}
	for _, field := range []string{w.PollInterval, w.LeaseDuration, w.LeaseMargin, w.HTTPTimeout, w.DBStatementTimeout} {
		if len(field) > 64 || strings.ContainsAny(field, "\r\n") {
			return errors.New("benchmark worker metadata is invalid")
		}
	}
	return nil
}

func (runner *Runner) Run(parent context.Context) (Report, string, error) {
	if err := ValidateOptions(runner.options); err != nil {
		return Report{}, "", err
	}
	runID, nonce, scenarioID, err := runIdentity(runner.options.RunID)
	if err != nil {
		return Report{}, "", err
	}
	if len(scenarioID) > 64 || len(runID) > maxRunIDLength {
		return Report{}, "", errors.New("benchmark run identity is too long")
	}
	if _, err = BuildIngestBody(scenarioID, "order-00001", runner.options.PayloadBytes); err != nil {
		return Report{}, "", err
	}
	runDirectory := filepath.Join(runner.outputRoot, runID)
	if err = os.MkdirAll(runner.outputRoot, 0750); err != nil {
		return Report{}, "", errors.New("benchmark artifact directory could not be created")
	}
	if err = os.Mkdir(runDirectory, 0750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return Report{}, "", errors.New("benchmark run-id already has an artifact directory; use a new run-id")
		}
		return Report{}, "", errors.New("benchmark run artifact directory could not be created")
	}

	startedWall := time.Now().UTC()
	report := Report{
		SchemaVersion: 1, RunID: runID, ScenarioID: scenarioID, Status: "running",
		StartedAt: startedWall, CommandArgs: runner.command,
		Configuration: ConfigurationReport{
			Events: runner.options.Events, ProducerConcurrency: runner.options.ProducerConcurrency,
			PayloadTargetBytes: runner.options.PayloadBytes, TimeoutSeconds: runner.options.Timeout.Seconds(),
			RunID: runID, ScenarioID: scenarioID,
			Destinations: []string{"fulfillment", "notification"},
			ReceiverMode: "normal", ReceiverDedupeEnabled: true,
			HTTPKeepAliveDisabled: true, RetryFailedRequests: false,
		},
		Environment:    runner.environment(),
		Counts:         CountsReport{PlannedRequests: runner.options.Events, RequestFailures: map[string]int{}},
		Reconciliation: ReconciliationReport{Issues: []string{}},
		Limitations: []string{
			"Finite closed-loop local workload; this run does not establish maximum sustainable throughput.",
			"API, worker, PostgreSQL, and receivers share one local host; receiver scenario-row locks and shared database work affect the measured path.",
			"Each planned ingest key is sent once. Failed or uncertain requests are reported and reconciled from PostgreSQL without replacement keys.",
		},
	}

	ctx, cancel := context.WithTimeout(parent, runner.options.Timeout)
	defer cancel()
	pool, err := openDatabase(ctx, runner.settings)
	if err != nil {
		report.fail("database_connect", "postgres_unavailable")
		return runner.finish(report, runDirectory, nil, nil, err)
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		report.fail("database_ping", "postgres_unavailable")
		return runner.finish(report, runDirectory, nil, nil, errors.New("benchmark PostgreSQL connection failed"))
	}
	if report.Environment.Database, err = databaseIdentity(ctx, pool); err != nil {
		report.fail("database_identity", "postgres_query_failed")
		return runner.finish(report, runDirectory, nil, nil, err)
	}
	keys := make([]string, runner.options.Events)
	keySequence := make(map[string]int, runner.options.Events)
	for i := range keys {
		key := idempotencyKey(runID, nonce, i+1)
		keys[i] = key
		keySequence[key] = i + 1
	}
	if err = ensureUnusedRun(ctx, pool, config.TenantID, scenarioID, keys); err != nil {
		report.fail("run_identity_check", "run_identity_conflict")
		return runner.finish(report, runDirectory, nil, nil, err)
	}
	if err = runner.createScenario(ctx, scenarioID); err != nil {
		report.fail("scenario_create", "receiver_scenario_unavailable")
		return runner.finish(report, runDirectory, nil, nil, err)
	}

	samples, ingestElapsed := runner.ingest(ctx, runID, nonce, scenarioID)
	report.Performance.IngestPhaseMilliseconds = milliseconds(ingestElapsed)
	for _, sample := range samples {
		if sample.Attempted {
			report.Counts.AttemptedRequests++
		}
		if sample.Outcome == "accepted" || sample.Outcome == "deduplicated" {
			report.Counts.SuccessfulResponses++
			if sample.Deduplicated {
				report.Counts.DeduplicatedResponses++
			}
		} else if sample.ErrorClass != "" {
			report.Counts.RequestFailures[sample.ErrorClass]++
		}
	}
	report.Counts.NotStartedRequests = runner.options.Events - report.Counts.AttemptedRequests

	drainStarted := time.Now()
	initialCtx, initialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	initialSnapshot, snapErr := collectSnapshot(initialCtx, pool, config.TenantID, scenarioID, keys)
	initialCancel()
	if snapErr != nil {
		report.fail("initial_reconciliation", "postgres_query_failed")
		return runner.finish(report, runDirectory, samples, nil, snapErr)
	}
	eventIDs := eventIDsOf(initialSnapshot.Events)
	terminal := allTerminal(initialSnapshot.Deliveries)
	var drainErr error
	if !terminal && ctx.Err() == nil {
		terminal, drainErr = runner.drain(ctx, pool, eventIDs)
	}
	report.Performance.DrainPhaseMilliseconds = milliseconds(time.Since(drainStarted))
	report.Drain.DurationMilliseconds = report.Performance.DrainPhaseMilliseconds
	if drainErr != nil {
		report.fail("drain", "postgres_query_failed")
	}
	if !terminal {
		report.Drain.DeadlineExceeded = errors.Is(ctx.Err(), context.DeadlineExceeded)
		report.Drain.Cancelled = errors.Is(ctx.Err(), context.Canceled)
	}

	finalCtx, finalCancel := context.WithTimeout(context.Background(), 5*time.Second)
	finalSnapshot, finalErr := collectSnapshot(finalCtx, pool, config.TenantID, scenarioID, keys)
	finalCancel()
	if finalErr != nil {
		report.fail("final_reconciliation", "postgres_query_failed")
		return runner.finish(report, runDirectory, samples, nil, finalErr)
	}
	samples = reconcileReport(&report, finalSnapshot, samples, keys, keySequence)
	report.Counts.LostAcknowledgments = countLostResponses(finalSnapshot.Events, samples, keySequence)
	populateCountsAndPerformance(&report, samples, finalSnapshot, ingestElapsed)
	report.Drain.Terminal = allTerminal(finalSnapshot.Deliveries)
	report.Drain.UnfinishedDenominator = report.Counts.PendingDeliveries + report.Counts.InFlightDeliveries
	report.Drain.DeadlineExceeded = errors.Is(ctx.Err(), context.DeadlineExceeded) && !report.Drain.Terminal
	report.Drain.Cancelled = errors.Is(ctx.Err(), context.Canceled) && !report.Drain.Terminal
	if report.Drain.DeadlineExceeded {
		report.fail("drain", "run_deadline_exceeded")
	}
	if report.Drain.Cancelled {
		report.fail("drain", "run_cancelled")
	}
	if !report.Reconciliation.Passed {
		report.fail("reconciliation", "durable_counts_do_not_match")
	}
	if report.Counts.AttemptedRequests != report.optionsEvents() {
		report.fail("ingest", "run_deadline_exceeded")
	}
	if hasRequestFailures(report.Counts.RequestFailures) && report.FailureCode == "" {
		report.fail("ingest", "client_requests_failed")
	}
	if report.FailureCode == "" {
		report.Status = "completed"
	} else if (report.FailureCode == "run_deadline_exceeded" || report.FailureCode == "run_cancelled") && (report.Counts.AttemptedRequests < report.Counts.PlannedRequests || !report.Drain.Terminal) {
		report.Status = "incomplete"
	} else {
		report.Status = "completed_with_errors"
	}
	return runner.finish(report, runDirectory, samples, finalSnapshot.Deliveries, nil)
}

func (report *Report) optionsEvents() int { return report.Configuration.Events }

func (report *Report) fail(step, code string) {
	if report.FailureCode == "" {
		report.FailureStep = step
		report.FailureCode = code
	}
}

func (runner *Runner) environment() EnvironmentReport {
	return EnvironmentReport{
		Host: runner.host, BenchmarkProcess: processMetadata(),
		Containers: runner.runtime.Services, Worker: runner.runtime.Worker,
		DatabasePoolMax: benchmarkDatabasePoolMax, APIDatabasePoolMax: 8, APIMetricsPoolMax: 1,
		WorkerMetricsPoolMax: 1, ReceiverDatabasePoolMax: 8,
		HTTPKeepAliveDisabled: true, WorkerSenderKeepAliveDisabled: true,
	}
}

func (runner *Runner) createScenario(ctx context.Context, scenarioID string) error {
	body, err := json.Marshal(struct {
		ScenarioID    string `json:"scenario_id"`
		Mode          string `json:"mode"`
		DedupeEnabled bool   `json:"dedupe_enabled"`
	}{scenarioID, "normal", true})
	if err != nil {
		return errors.New("benchmark receiver scenario encoding failed")
	}
	for _, code := range []string{"fulfillment", "notification"} {
		if err = runner.controlRequest(ctx, code, body); err != nil {
			return errors.New("benchmark receiver scenario creation failed")
		}
	}
	return nil
}

func (runner *Runner) controlRequest(ctx context.Context, code string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, runner.endpoints.Controls[code]+"/control/scenarios", bytes.NewReader(body))
	if err != nil {
		return errors.New("benchmark receiver control request configuration failed")
	}
	req.Header.Set("Authorization", "Bearer "+runner.settings.DemoControlToken())
	req.Header.Set("Content-Type", "application/json")
	response, err := runner.client.Do(req)
	if err != nil {
		return errors.New("benchmark receiver control request failed")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBytes))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("benchmark receiver control request was rejected")
	}
	return nil
}

func (runner *Runner) ingest(ctx context.Context, runID, nonce, scenarioID string) ([]requestSample, time.Duration) {
	samples := make([]requestSample, runner.options.Events)
	for i := range samples {
		samples[i] = requestSample{Sequence: i + 1, Outcome: "not_started"}
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	for i := 0; i < runner.options.ProducerConcurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for sequence := range jobs {
				if ctx.Err() != nil {
					continue
				}
				samples[sequence-1] = runner.sendIngest(ctx, runID, nonce, scenarioID, sequence)
			}
		}()
	}
	started := time.Now()
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		defer close(jobs)
		for sequence := 1; sequence <= runner.options.Events; sequence++ {
			select {
			case jobs <- sequence:
			case <-ctx.Done():
				return
			}
		}
	}()
	workers.Wait()
	<-producerDone
	return samples, time.Since(started)
}

func (runner *Runner) sendIngest(ctx context.Context, runID, nonce, scenarioID string, sequence int) requestSample {
	sample := requestSample{Sequence: sequence, Outcome: "not_started"}
	orderID := fmt.Sprintf("order-%05d", sequence)
	body, err := BuildIngestBody(scenarioID, orderID, runner.options.PayloadBytes)
	if err != nil {
		sample.ErrorClass = "request_build_failed"
		sample.Outcome = "local_error"
		return sample
	}
	sample.RequestBytes = len(body)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, runner.endpoints.API+"/v1/events", bytes.NewReader(body))
	if err != nil {
		sample.ErrorClass = "request_build_failed"
		sample.Outcome = "local_error"
		return sample
	}
	request.Header.Set("Authorization", "Bearer "+runner.settings.APIToken())
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey(runID, nonce, sequence))
	sample.Attempted = true
	started := time.Now()
	response, err := runner.client.Do(request)
	sample.LatencyNS = time.Since(started).Nanoseconds()
	if err != nil {
		sample.ErrorClass = transportErrorClass(err)
		sample.Outcome = sample.ErrorClass
		return sample
	}
	defer response.Body.Close()
	sample.HTTPStatus = response.StatusCode
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusOK {
		sample.ErrorClass = httpErrorClass(response.StatusCode)
		sample.Outcome = sample.ErrorClass
		return sample
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(encoded) > maxResponseBytes {
		sample.ErrorClass = "invalid_response"
		sample.Outcome = "invalid_response"
		return sample
	}
	var result struct {
		EventID      string    `json:"event_id"`
		Deduplicated bool      `json:"deduplicated"`
		CreatedAt    time.Time `json:"created_at"`
		DeliveryIDs  []string  `json:"delivery_ids"`
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&result); err != nil || uuid(result.EventID) == false || len(result.DeliveryIDs) != 2 || !validResponseTime(result.CreatedAt) || result.Deduplicated != (response.StatusCode == http.StatusOK) {
		sample.ErrorClass = "invalid_response"
		sample.Outcome = "invalid_response"
		return sample
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		sample.ErrorClass = "invalid_response"
		sample.Outcome = "invalid_response"
		return sample
	}
	for _, id := range result.DeliveryIDs {
		if !uuid(id) {
			sample.ErrorClass = "invalid_response"
			sample.Outcome = "invalid_response"
			return sample
		}
	}
	if result.DeliveryIDs[0] == result.DeliveryIDs[1] {
		sample.ErrorClass = "invalid_response"
		sample.Outcome = "invalid_response"
		return sample
	}
	sample.EventID = result.EventID
	sample.DeliveryIDs = result.DeliveryIDs
	sample.Deduplicated = result.Deduplicated
	if result.Deduplicated {
		sample.Outcome = "deduplicated"
	} else {
		sample.Outcome = "accepted"
	}
	return sample
}

func validResponseTime(value time.Time) bool { return !value.IsZero() }

func transportErrorClass(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "transport_error"
}

func httpErrorClass(status int) string {
	switch {
	case status == http.StatusTooManyRequests:
		return "http_429"
	case status >= 500:
		return "http_5xx"
	default:
		return "http_other"
	}
}

func (runner *Runner) drain(ctx context.Context, pool *pgxpool.Pool, eventIDs []string) (bool, error) {
	for {
		progress, err := deliveryProgress(ctx, pool, eventIDs)
		if err != nil {
			return false, err
		}
		if progress.pending+progress.inFlight == 0 {
			return true, nil
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, nil
		case <-timer.C:
		}
	}
}

func (runner *Runner) finish(report Report, runDirectory string, requests []requestSample, deliveries []deliveryRow, runErr error) (Report, string, error) {
	if report.Counts.RequestFailures == nil {
		report.Counts.RequestFailures = map[string]int{}
	}
	if report.Reconciliation.Issues == nil {
		report.Reconciliation.Issues = []string{}
	}
	if report.Performance.Destinations == nil {
		report.Performance.Destinations = map[string]DestinationReport{}
	}
	if report.Performance.Definitions == nil {
		report.Performance.Definitions = metricDefinitions()
	}
	if report.Counts.PlannedRequests > report.Counts.AttemptedRequests {
		report.Counts.NotStartedRequests = report.Counts.PlannedRequests - report.Counts.AttemptedRequests
	}
	if report.Status == "running" {
		if runErr != nil {
			report.Status = "failed"
		} else {
			report.Status = "completed_with_errors"
		}
	}
	report.FinishedAt = time.Now().UTC()
	relative := filepath.ToSlash(filepath.Join(runner.outputRoot, filepath.Base(runDirectory)))
	report.Artifacts = ArtifactFiles{
		Report:          filepath.ToSlash(filepath.Join(relative, "report.json")),
		IngestSamples:   filepath.ToSlash(filepath.Join(relative, "ingest_samples.csv")),
		DeliverySamples: filepath.ToSlash(filepath.Join(relative, "delivery_samples.csv")),
	}
	if err := writeIngestSamples(filepath.Join(runDirectory, "ingest_samples.csv"), requests); err != nil {
		return report, report.Artifacts.Report, errors.New("benchmark ingest samples could not be written")
	}
	if err := writeDeliverySamples(filepath.Join(runDirectory, "delivery_samples.csv"), deliveries); err != nil {
		return report, report.Artifacts.Report, errors.New("benchmark delivery samples could not be written")
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return report, report.Artifacts.Report, errors.New("benchmark report encoding failed")
	}
	if err = os.WriteFile(filepath.Join(runDirectory, "report.json"), append(encoded, '\n'), 0640); err != nil {
		return report, report.Artifacts.Report, errors.New("benchmark report could not be written")
	}
	if runErr != nil {
		return report, report.Artifacts.Report, runErr
	}
	if report.Status != "completed" {
		return report, report.Artifacts.Report, errors.New("benchmark completed with errors; inspect the sanitized report")
	}
	return report, report.Artifacts.Report, nil
}

func metricDefinitions() map[string]string {
	return map[string]string{
		"percentiles":                       "Nearest rank on sorted samples: rank = ceil(p*n), one-based; p50, p95, and p99 are reported.",
		"api_request_latency":               "Monotonic client elapsed time for every attempted ingest request; success-only includes valid HTTP 200 or 202 responses.",
		"accepted_unique_events_per_second": "Unique events reconciled from PostgreSQL for this run divided by monotonic ingest phase duration.",
		"initial_queue_delay":               "First persisted attempt started_at minus event created_at, using PostgreSQL timestamps from the same database clock.",
		"end_to_end_acknowledgment_latency": "For succeeded deliveries only, delivery confirmed_at minus event created_at using PostgreSQL timestamps; dead and unfinished deliveries remain in the denominator counts.",
		"retry_amplification":               "Persisted attempts divided by unique deliveries; an attempt row can exist even if the worker dies before sending HTTP.",
		"receiver_request_amplification":    "Persisted receiver request rows divided by unique deliveries; receiver effects are counted separately.",
		"input_and_wire_sizes":              "Input size is the serialized ingest request before the API constructs its outgoing envelope; wire size is the stored event wire_body length.",
		"request_retry_policy":              "The benchmark sends each planned idempotency key once; failed and uncertain requests are recorded and never replaced with new keys.",
		"unfinished_denominator":            "Pending and in-flight deliveries at the final database snapshot; dead deliveries are reported separately.",
	}
}

func idempotencyKey(runID, nonce string, sequence int) string {
	return fmt.Sprintf("bench:%s:%s:%05d", runID, nonce, sequence)
}

func eventIDsOf(events []eventRow) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.ID)
	}
	return ids
}

func uuid(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if char != '-' {
				return false
			}
		} else if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func milliseconds(duration time.Duration) float64 {
	return round3(float64(duration) / float64(time.Millisecond))
}

func round3(value float64) float64 { return math.Round(value*1000) / 1000 }

func hasRequestFailures(failures map[string]int) bool {
	for _, count := range failures {
		if count > 0 {
			return true
		}
	}
	return false
}

func countLostResponses(events []eventRow, samples []requestSample, keySequence map[string]int) int {
	count := 0
	for _, event := range events {
		sequence, ok := keySequence[event.IdempotencyKey]
		if !ok || sequence < 1 || sequence > len(samples) || samples[sequence-1].EventID == "" {
			count++
		}
	}
	return count
}

func allTerminal(deliveries []deliveryRow) bool {
	for _, delivery := range deliveries {
		if delivery.Status == "pending" || delivery.Status == "in_flight" {
			return false
		}
	}
	return true
}

func sortedIDs(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
