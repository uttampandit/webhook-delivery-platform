package benchmark

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

const (
	DefaultEvents       = 200
	MaxEvents           = 10000
	DefaultConcurrency  = 4
	MaxConcurrency      = 32
	DefaultPayloadBytes = 1024
	MaxPayloadBytes     = 32768
	DefaultTimeout      = 180 * time.Second
	MaxTimeout          = 30 * time.Minute
)

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,40}$`)

type Options struct {
	Events              int
	ProducerConcurrency int
	PayloadBytes        int
	Timeout             time.Duration
	RunID               string
	Command             []string
}

type Endpoints struct {
	API      string
	Controls map[string]string
}

func DefaultEndpoints(mode config.RunMode) (Endpoints, error) {
	switch mode {
	case config.RunModeCompose:
		return Endpoints{
			API: "http://api:8080",
			Controls: map[string]string{
				"fulfillment":  "http://fulfillment:9091",
				"notification": "http://notification:9091",
			},
		}, nil
	case config.RunModeHost:
		return Endpoints{
			API: "http://127.0.0.1:8080",
			Controls: map[string]string{
				"fulfillment":  "http://127.0.0.1:9092",
				"notification": "http://127.0.0.1:9093",
			},
		}, nil
	default:
		return Endpoints{}, errors.New("benchmark run mode is invalid")
	}
}

type HostMetadata struct {
	Platform      string `json:"platform"`
	OS            string `json:"os"`
	Release       string `json:"release"`
	Architecture  string `json:"architecture"`
	CPUCount      int    `json:"cpu_count"`
	MemoryBytes   int64  `json:"memory_bytes"`
	GoVersion     string `json:"go_version"`
	NodeVersion   string `json:"node_version"`
	SourceCommit  string `json:"source_commit"`
	WorktreeDirty bool   `json:"worktree_dirty"`
}

type ContainerMetadata struct {
	ImageRef         string `json:"image_ref"`
	ImageID          string `json:"image_id"`
	NanoCPUs         int64  `json:"nano_cpus"`
	CPUQuota         int64  `json:"cpu_quota"`
	CPUPeriod        int64  `json:"cpu_period"`
	CPUSet           string `json:"cpu_set"`
	MemoryLimitBytes int64  `json:"memory_limit_bytes"`
}

type WorkerConfiguration struct {
	Concurrency        int    `json:"concurrency"`
	PollInterval       string `json:"poll_interval"`
	LeaseDuration      string `json:"lease_duration"`
	LeaseMargin        string `json:"lease_margin"`
	HTTPTimeout        string `json:"http_timeout"`
	DBStatementTimeout string `json:"db_statement_timeout"`
	DatabasePoolMax    int    `json:"database_pool_max"`
}

type RuntimeMetadata struct {
	Services map[string]ContainerMetadata `json:"services"`
	Worker   WorkerConfiguration          `json:"worker_configuration"`
}

type EnvironmentReport struct {
	Host                          HostMetadata                 `json:"host"`
	BenchmarkProcess              ProcessMetadata              `json:"benchmark_process"`
	Database                      DatabaseMetadata             `json:"database"`
	Containers                    map[string]ContainerMetadata `json:"containers"`
	Worker                        WorkerConfiguration          `json:"worker_configuration"`
	DatabasePoolMax               int                          `json:"benchmark_database_pool_max"`
	APIDatabasePoolMax            int                          `json:"api_database_pool_max"`
	APIMetricsPoolMax             int                          `json:"api_metrics_database_pool_max"`
	WorkerMetricsPoolMax          int                          `json:"worker_metrics_database_pool_max"`
	ReceiverDatabasePoolMax       int                          `json:"receiver_database_pool_max"`
	HTTPKeepAliveDisabled         bool                         `json:"benchmark_http_keepalive_disabled"`
	WorkerSenderKeepAliveDisabled bool                         `json:"worker_sender_http_keepalive_disabled"`
}

type ProcessMetadata struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	GoVersion    string `json:"go_version"`
}

type DatabaseMetadata struct {
	Name          string `json:"name"`
	ServerVersion string `json:"server_version"`
}

type ConfigurationReport struct {
	Events                int      `json:"requested_events"`
	ProducerConcurrency   int      `json:"producer_concurrency"`
	PayloadTargetBytes    int      `json:"payload_target_bytes"`
	TimeoutSeconds        float64  `json:"timeout_seconds"`
	RunID                 string   `json:"run_id"`
	ScenarioID            string   `json:"scenario_id"`
	Destinations          []string `json:"destinations"`
	ReceiverMode          string   `json:"receiver_mode"`
	ReceiverDedupeEnabled bool     `json:"receiver_dedupe_enabled"`
	HTTPKeepAliveDisabled bool     `json:"http_keepalive_disabled"`
	RetryFailedRequests   bool     `json:"retry_failed_requests"`
}

type CountsReport struct {
	PlannedRequests       int            `json:"planned_requests"`
	AttemptedRequests     int            `json:"attempted_requests"`
	NotStartedRequests    int            `json:"not_started_requests"`
	SuccessfulResponses   int            `json:"successful_responses"`
	DeduplicatedResponses int            `json:"deduplicated_responses"`
	RequestFailures       map[string]int `json:"request_failures_by_class"`
	AcceptedUniqueEvents  int            `json:"accepted_unique_events_from_database"`
	Deliveries            int            `json:"unique_deliveries"`
	SucceededDeliveries   int            `json:"succeeded_deliveries"`
	DeadDeliveries        int            `json:"dead_deliveries"`
	PendingDeliveries     int            `json:"pending_deliveries"`
	InFlightDeliveries    int            `json:"in_flight_deliveries"`
	PersistedAttempts     int            `json:"persisted_attempts"`
	ReceiverRequests      int64          `json:"persisted_receiver_requests"`
	ReceiverEffects       int64          `json:"persisted_receiver_effects"`
	LostAcknowledgments   int            `json:"accepted_events_without_valid_api_response"`
}

type DrainReport struct {
	DurationMilliseconds  float64 `json:"duration_ms"`
	Terminal              bool    `json:"all_existing_deliveries_terminal"`
	DeadlineExceeded      bool    `json:"run_deadline_exceeded"`
	Cancelled             bool    `json:"run_cancelled"`
	UnfinishedDenominator int     `json:"unfinished_denominator_deliveries"`
}

type DestinationReport struct {
	Deliveries              int          `json:"unique_deliveries"`
	Succeeded               int          `json:"succeeded"`
	Dead                    int          `json:"dead"`
	Unfinished              int          `json:"unfinished"`
	PersistedAttempts       int          `json:"persisted_attempts"`
	ReceiverRequests        int64        `json:"persisted_receiver_requests"`
	ReceiverEffects         int64        `json:"persisted_receiver_effects"`
	RetryAmplification      *float64     `json:"retry_amplification"`
	RequestAmplification    *float64     `json:"receiver_request_amplification"`
	InitialQueueDelayMS     Distribution `json:"initial_queue_delay_ms"`
	AcknowledgmentLatencyMS Distribution `json:"acknowledgment_latency_ms"`
}

type PerformanceReport struct {
	IngestPhaseMilliseconds      float64                      `json:"ingest_phase_duration_ms"`
	DrainPhaseMilliseconds       float64                      `json:"drain_phase_duration_ms"`
	AcceptedEventsPerSecond      float64                      `json:"accepted_unique_events_per_second"`
	RetryAmplification           *float64                     `json:"retry_amplification"`
	ReceiverRequestAmplification *float64                     `json:"receiver_request_amplification"`
	APIRequestLatencyAllMS       Distribution                 `json:"api_request_latency_all_ms"`
	APIRequestLatencySuccessMS   Distribution                 `json:"api_request_latency_success_only_ms"`
	InitialQueueDelayMS          Distribution                 `json:"initial_queue_delay_ms"`
	AcknowledgmentLatencyMS      Distribution                 `json:"end_to_end_acknowledgment_latency_ms"`
	InputRequestBytes            Distribution                 `json:"actual_input_request_bytes"`
	StoredRequestBytes           Distribution                 `json:"database_stored_request_bytes"`
	WireBodyBytes                Distribution                 `json:"actual_wire_body_bytes"`
	Destinations                 map[string]DestinationReport `json:"by_destination"`
	Definitions                  map[string]string            `json:"definitions"`
}

type ReconciliationReport struct {
	APIResponseEventIDsMatchDatabase bool     `json:"api_response_event_ids_match_database"`
	ReturnedDeliveryIDsMatchDatabase bool     `json:"api_returned_delivery_ids_match_database"`
	DeliveryCountMatchesAccepted     bool     `json:"delivery_count_matches_accepted_events"`
	ReceiverConfigurationMatches     bool     `json:"receiver_scenarios_are_normal_with_dedupe"`
	ReceiverOperationsMatch          bool     `json:"receiver_operations_match_deliveries"`
	ReceiverEffectsDeduplicated      bool     `json:"receiver_effects_are_deduplicated_per_operation"`
	ExpectedDeliveries               int      `json:"expected_deliveries"`
	Issues                           []string `json:"issues"`
	Passed                           bool     `json:"passed"`
}

type ArtifactFiles struct {
	Report          string `json:"report"`
	IngestSamples   string `json:"ingest_samples"`
	DeliverySamples string `json:"delivery_samples"`
}

type Report struct {
	SchemaVersion  int                  `json:"schema_version"`
	RunID          string               `json:"run_id"`
	ScenarioID     string               `json:"scenario_id"`
	Status         string               `json:"status"`
	StartedAt      time.Time            `json:"started_at"`
	FinishedAt     time.Time            `json:"finished_at"`
	FailureStep    string               `json:"failure_step,omitempty"`
	FailureCode    string               `json:"failure_code,omitempty"`
	CommandArgs    []string             `json:"command_args"`
	Configuration  ConfigurationReport  `json:"configuration"`
	Environment    EnvironmentReport    `json:"environment"`
	Counts         CountsReport         `json:"counts"`
	Drain          DrainReport          `json:"drain"`
	Performance    PerformanceReport    `json:"performance"`
	Reconciliation ReconciliationReport `json:"reconciliation"`
	Artifacts      ArtifactFiles        `json:"raw_sample_files"`
	Limitations    []string             `json:"limitations"`
}

type requestSample struct {
	Sequence             int
	Attempted            bool
	HTTPStatus           int
	LatencyNS            int64
	Outcome              string
	ErrorClass           string
	EventID              string
	DeliveryIDs          []string
	Deduplicated         bool
	RequestBytes         int
	DatabaseEventID      string
	DatabaseRequestBytes int
}

type eventRow struct {
	ID             string
	IdempotencyKey string
	CreatedAt      time.Time
	RequestBytes   int
	WireBytes      int
}

type deliveryRow struct {
	ID          string
	EventID     string
	Destination string
	Status      string
	CreatedAt   time.Time
	FirstStart  *time.Time
	ConfirmedAt *time.Time
	Attempts    int
}

type receiverRow struct {
	Code                  string
	Mode                  string
	Dedupe                bool
	Requests              int64
	RequestOperationCount int64
	RequestOps            []string
	Effects               int64
	EffectOperationCount  int64
	EffectOps             []string
}

type snapshot struct {
	DatabaseName string
	DBVersion    string
	Events       []eventRow
	Deliveries   []deliveryRow
	Receivers    map[string]receiverRow
}

func ValidateOptions(options Options) error {
	if options.Events < 1 || options.Events > MaxEvents {
		return fmt.Errorf("events must be between 1 and %d", MaxEvents)
	}
	if options.ProducerConcurrency < 1 || options.ProducerConcurrency > MaxConcurrency {
		return fmt.Errorf("producer-concurrency must be between 1 and %d", MaxConcurrency)
	}
	if options.PayloadBytes < 1 || options.PayloadBytes > MaxPayloadBytes {
		return fmt.Errorf("payload-bytes must be between 1 and %d", MaxPayloadBytes)
	}
	if options.Timeout <= 0 || options.Timeout > MaxTimeout {
		return fmt.Errorf("timeout must be positive and at most %s", MaxTimeout)
	}
	if options.RunID != "" && !runIDPattern.MatchString(options.RunID) {
		return errors.New("run-id must contain 1 to 40 ASCII letters, digits, underscores, or hyphens")
	}
	return nil
}

func runIdentity(requested string) (runID, nonce, scenarioID string, err error) {
	var suffix [8]byte
	if _, err = rand.Read(suffix[:]); err != nil {
		return "", "", "", errors.New("benchmark run identity unavailable")
	}
	if requested == "" {
		runID = "bench_" + time.Now().UTC().Format("20060102T150405Z") + "_" + hex.EncodeToString(suffix[:4])
	} else {
		runID = requested
	}
	nonce = hex.EncodeToString(suffix[:])
	scenarioID = "bench_" + runID + "_" + nonce
	if len(scenarioID) > 64 {
		return "", "", "", errors.New("run-id is too long for a receiver scenario identifier")
	}
	return runID, nonce, scenarioID, nil
}

func processMetadata() ProcessMetadata {
	return ProcessMetadata{OS: runtime.GOOS, Architecture: runtime.GOARCH, GoVersion: runtime.Version()}
}
