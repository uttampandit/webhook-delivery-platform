package config

import (
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Service identifies which secrets a process consumes. All services still
// validate the shared database, run mode, and timing settings at startup.
type Service string

const (
	ServiceAPI       Service = "api"
	ServiceWorker    Service = "worker"
	ServiceReceiver  Service = "receiver"
	ServiceSeed      Service = "seed"
	ServiceMigrate   Service = "migrate"
	ServiceDemoctl   Service = "democtl"
	ServiceBenchmark Service = "benchmark"
)

type RunMode string

const (
	RunModeCompose RunMode = "compose"
	RunModeHost    RunMode = "host"
)

const (
	TenantID                = "00000000-0000-4000-8000-000000000001"
	FulfillmentDestination  = "00000000-0000-4000-8000-000000000101"
	NotificationDestination = "00000000-0000-4000-8000-000000000102"
	FulfillmentKeyRef       = "fulfillment-v1"
	NotificationKeyRef      = "notification-v1"
)

const (
	defaultWorkerConcurrency = 4
	defaultWorkerPoll        = 250 * time.Millisecond
	defaultLease             = 30 * time.Second
	defaultLeaseMargin       = 5 * time.Second
	defaultHTTPTimeout       = 5 * time.Second
	defaultReaperInterval    = time.Second
	defaultDBStatement       = 3 * time.Second
	minimumSecretBytes       = 32
)

// Settings contains parsed values for one process. Secrets stay private and
// are exposed only through accessors that return copies where applicable.
type Settings struct {
	databaseURL        string
	runMode            RunMode
	workerConcurrency  int
	workerPollInterval time.Duration
	leaseDuration      time.Duration
	leaseMargin        time.Duration
	httpTimeout        time.Duration
	reaperInterval     time.Duration
	dbStatementTimeout time.Duration
	apiToken           string
	demoControlToken   string
	signingKeys        map[string][]byte
	receiverCode       string
}

// Load reads the process environment once and validates it for service.
func Load(service Service) (Settings, error) {
	return LoadFrom(service, os.LookupEnv)
}

// LoadFrom parses a supplied environment lookup. It is useful for deterministic
// startup tests without mutating the process environment.
func LoadFrom(service Service, lookup func(string) (string, bool)) (Settings, error) {
	if lookup == nil {
		return Settings{}, errors.New("configuration environment lookup is unavailable")
	}
	if !knownService(service) {
		return Settings{}, errors.New("configuration service is invalid")
	}

	databaseURL, ok := lookup("DATABASE_URL")
	if !ok || strings.TrimSpace(databaseURL) == "" || !validDatabaseURL(strings.TrimSpace(databaseURL)) {
		return Settings{}, errors.New("DATABASE_URL is required and must be a PostgreSQL URL")
	}

	modeText, ok := lookup("RUN_MODE")
	if !ok {
		return Settings{}, errors.New("RUN_MODE is required and must be compose or host")
	}
	mode := RunMode(strings.TrimSpace(modeText))
	if mode != RunModeCompose && mode != RunModeHost {
		return Settings{}, errors.New("RUN_MODE must be compose or host")
	}

	concurrency, err := readConcurrency(lookup)
	if err != nil {
		return Settings{}, err
	}
	pollInterval, err := readDuration(lookup, "WORKER_POLL_INTERVAL", defaultWorkerPoll)
	if err != nil {
		return Settings{}, err
	}
	leaseDuration, err := readDuration(lookup, "LEASE_DURATION", defaultLease)
	if err != nil {
		return Settings{}, err
	}
	leaseMargin, err := readDuration(lookup, "LEASE_MARGIN", defaultLeaseMargin)
	if err != nil {
		return Settings{}, err
	}
	httpTimeout, err := readDuration(lookup, "HTTP_TIMEOUT", defaultHTTPTimeout)
	if err != nil {
		return Settings{}, err
	}
	reaperInterval, err := readDuration(lookup, "REAPER_INTERVAL", defaultReaperInterval)
	if err != nil {
		return Settings{}, err
	}
	dbStatementTimeout, err := readDuration(lookup, "DB_STATEMENT_TIMEOUT", defaultDBStatement)
	if err != nil {
		return Settings{}, err
	}
	if httpTimeout >= leaseDuration || leaseMargin >= leaseDuration-httpTimeout {
		return Settings{}, errors.New("HTTP_TIMEOUT plus LEASE_MARGIN must be less than LEASE_DURATION")
	}

	settings := Settings{
		databaseURL:        strings.TrimSpace(databaseURL),
		runMode:            mode,
		workerConcurrency:  concurrency,
		workerPollInterval: pollInterval,
		leaseDuration:      leaseDuration,
		leaseMargin:        leaseMargin,
		httpTimeout:        httpTimeout,
		reaperInterval:     reaperInterval,
		dbStatementTimeout: dbStatementTimeout,
		signingKeys:        make(map[string][]byte),
	}

	if service == ServiceAPI || service == ServiceDemoctl || service == ServiceBenchmark {
		settings.apiToken, _, err = readSecret(lookup, "API_TOKEN")
		if err != nil {
			return Settings{}, err
		}
	}
	if service == ServiceDemoctl || service == ServiceReceiver || service == ServiceBenchmark {
		settings.demoControlToken, _, err = readSecret(lookup, "DEMO_CONTROL_TOKEN")
		if err != nil {
			return Settings{}, err
		}
	}
	if service == ServiceWorker {
		if err := settings.readSigningKey(lookup, "FULFILLMENT_SIGNING_KEY_V1", FulfillmentKeyRef); err != nil {
			return Settings{}, err
		}
		if err := settings.readSigningKey(lookup, "NOTIFICATION_SIGNING_KEY_V1", NotificationKeyRef); err != nil {
			return Settings{}, err
		}
	}
	if service == ServiceReceiver {
		code, ok := lookup("RECEIVER_CODE")
		if !ok {
			return Settings{}, errors.New("RECEIVER_CODE is required and must be fulfillment or notification")
		}
		settings.receiverCode = strings.TrimSpace(code)
		switch settings.receiverCode {
		case "fulfillment":
			if err := settings.readSigningKey(lookup, "FULFILLMENT_SIGNING_KEY_V1", FulfillmentKeyRef); err != nil {
				return Settings{}, err
			}
		case "notification":
			if err := settings.readSigningKey(lookup, "NOTIFICATION_SIGNING_KEY_V1", NotificationKeyRef); err != nil {
				return Settings{}, err
			}
		default:
			return Settings{}, errors.New("RECEIVER_CODE must be fulfillment or notification")
		}
	}

	return settings, nil
}

func knownService(service Service) bool {
	switch service {
	case ServiceAPI, ServiceWorker, ServiceReceiver, ServiceSeed, ServiceMigrate, ServiceDemoctl, ServiceBenchmark:
		return true
	default:
		return false
	}
}

func validDatabaseURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return false
	}
	return parsed.Hostname() != "" && parsed.Path != "" && parsed.Path != "/"
}

func readConcurrency(lookup func(string) (string, bool)) (int, error) {
	raw, ok := lookup("WORKER_CONCURRENCY")
	if !ok || strings.TrimSpace(raw) == "" {
		return defaultWorkerConcurrency, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 1 || value > 32 {
		return 0, errors.New("WORKER_CONCURRENCY must be an integer from 1 through 32")
	}
	return value, nil
}

func readDuration(lookup func(string) (string, bool), name string, fallback time.Duration) (time.Duration, error) {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, errors.New(name + " must be a positive Go duration")
	}
	return value, nil
}

func readSecret(lookup func(string) (string, bool), name string) (string, []byte, error) {
	raw, ok := lookup(name)
	if !ok || raw == "" {
		return "", nil, errors.New(name + " is required")
	}
	if strings.TrimSpace(raw) != raw {
		return "", nil, errors.New(name + " must be a hexadecimal secret with at least 32 bytes")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) < minimumSecretBytes {
		return "", nil, errors.New(name + " must be a hexadecimal secret with at least 32 bytes")
	}
	return raw, decoded, nil
}

func (settings *Settings) readSigningKey(lookup func(string) (string, bool), envName, keyRef string) error {
	_, key, err := readSecret(lookup, envName)
	if err != nil {
		return err
	}
	if len(key) != 32 {
		return errors.New(envName + " must be a hexadecimal secret with exactly 32 bytes")
	}
	settings.signingKeys[keyRef] = key
	return nil
}

func (settings Settings) DatabaseURL() string               { return settings.databaseURL }
func (settings Settings) RunMode() RunMode                  { return settings.runMode }
func (settings Settings) WorkerConcurrency() int            { return settings.workerConcurrency }
func (settings Settings) WorkerPollInterval() time.Duration { return settings.workerPollInterval }
func (settings Settings) LeaseDuration() time.Duration      { return settings.leaseDuration }
func (settings Settings) LeaseMargin() time.Duration        { return settings.leaseMargin }
func (settings Settings) HTTPTimeout() time.Duration        { return settings.httpTimeout }
func (settings Settings) ReaperInterval() time.Duration     { return settings.reaperInterval }
func (settings Settings) DBStatementTimeout() time.Duration { return settings.dbStatementTimeout }
func (settings Settings) APIToken() string                  { return settings.apiToken }
func (settings Settings) DemoControlToken() string          { return settings.demoControlToken }
func (settings Settings) ReceiverCode() string              { return settings.receiverCode }

// SigningKey returns a copy so callers cannot mutate the parsed key held by
// the settings value.
func (settings Settings) SigningKey(keyRef string) ([]byte, bool) {
	key, ok := settings.signingKeys[keyRef]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), key...), true
}

// RegistryForMode returns the fixed local destination IDs and URLs for mode.
// There is intentionally no URL override in environment variables or flags.
func RegistryForMode(mode RunMode) (Registry, error) {
	var fulfillmentURL, notificationURL string
	switch mode {
	case RunModeCompose:
		fulfillmentURL = "http://fulfillment:8081/webhook"
		notificationURL = "http://notification:8081/webhook"
	case RunModeHost:
		fulfillmentURL = "http://127.0.0.1:8082/webhook"
		notificationURL = "http://127.0.0.1:8083/webhook"
	default:
		return Registry{}, errors.New("run mode must be compose or host")
	}
	return Registry{
		tenant: Tenant{ID: TenantID, Name: "Webhook Delivery Demo"},
		destinations: []Destination{
			{ID: FulfillmentDestination, TenantID: TenantID, Code: "fulfillment", URL: fulfillmentURL, KeyRef: FulfillmentKeyRef},
			{ID: NotificationDestination, TenantID: TenantID, Code: "notification", URL: notificationURL, KeyRef: NotificationKeyRef},
		},
	}, nil
}

type Tenant struct {
	ID   string
	Name string
}

type Destination struct {
	ID       string
	TenantID string
	Code     string
	URL      string
	KeyRef   string
}

// Registry keeps its stored values private. Destinations returns a copy so a
// caller cannot mutate the registry that another component will seed.
type Registry struct {
	tenant       Tenant
	destinations []Destination
}

func (registry Registry) Tenant() Tenant { return registry.tenant }

func (registry Registry) Destinations() []Destination {
	return append([]Destination(nil), registry.destinations...)
}

// Registry returns the fixed rows chosen by this configuration's run mode.
func (settings Settings) Registry() Registry {
	registry, _ := RegistryForMode(settings.runMode)
	return registry
}

// PostgreSQLStatementTimeout returns integer milliseconds in PostgreSQL's
// supported positive range. Round up so sub-millisecond settings never disable
// the server timeout (zero). The Go operation context retains the exact duration.
func (settings Settings) PostgreSQLStatementTimeout() string {
	ms := settings.dbStatementTimeout / time.Millisecond
	if settings.dbStatementTimeout%time.Millisecond != 0 {
		ms++
	}
	if ms < 1 {
		ms = 1
	}
	if ms > 2147483647 {
		ms = 2147483647
	}
	return strconv.FormatInt(int64(ms), 10)
}
