package config

import (
	"strings"
	"testing"
	"time"
)

const validTestSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestLoadUsesDefaultsAndRoleSpecificSecrets(t *testing.T) {
	values := validEnvironment()
	delete(values, "API_TOKEN")
	delete(values, "DEMO_CONTROL_TOKEN")
	settings, err := LoadFrom(ServiceWorker, mapLookup(values))
	if err != nil {
		t.Fatal("load worker settings:", err)
	}
	if settings.WorkerConcurrency() != 4 || settings.WorkerPollInterval() != 250*time.Millisecond {
		t.Fatalf("worker defaults = (%d, %s), want (4, 250ms)", settings.WorkerConcurrency(), settings.WorkerPollInterval())
	}
	if settings.LeaseDuration() != 30*time.Second || settings.LeaseMargin() != 5*time.Second || settings.HTTPTimeout() != 5*time.Second {
		t.Fatalf("lease defaults = (%s, %s, %s), want (30s, 5s, 5s)", settings.LeaseDuration(), settings.LeaseMargin(), settings.HTTPTimeout())
	}
	if _, ok := settings.SigningKey(FulfillmentKeyRef); !ok {
		t.Fatal("worker settings do not contain fulfillment signing key")
	}
	if _, ok := settings.SigningKey(NotificationKeyRef); !ok {
		t.Fatal("worker settings do not contain notification signing key")
	}
	if settings.APIToken() != "" || settings.DemoControlToken() != "" {
		t.Fatal("worker unexpectedly loaded operator tokens")
	}

	apiValues := validEnvironment()
	delete(apiValues, "FULFILLMENT_SIGNING_KEY_V1")
	delete(apiValues, "NOTIFICATION_SIGNING_KEY_V1")
	api, err := LoadFrom(ServiceAPI, mapLookup(apiValues))
	if err != nil {
		t.Fatal("load API settings:", err)
	}
	if api.APIToken() != validTestSecret {
		t.Fatal("API token was not retained for the API process")
	}

	benchmark, err := LoadFrom(ServiceBenchmark, mapLookup(validEnvironment()))
	if err != nil {
		t.Fatal("load benchmark settings:", err)
	}
	if benchmark.APIToken() != validTestSecret || benchmark.DemoControlToken() != validTestSecret {
		t.Fatal("benchmark settings did not load both required control tokens")
	}
	if _, ok := benchmark.SigningKey(FulfillmentKeyRef); ok {
		t.Fatal("benchmark unexpectedly loaded receiver signing keys")
	}
}

func TestLoadRejectsMissingInvalidAndShortSecretsWithoutDisclosure(t *testing.T) {
	const planted = "planted-secret-value-that-must-never-appear-in-errors"
	cases := []struct {
		name    string
		service Service
		key     string
		value   string
	}{
		{name: "missing API token", service: ServiceAPI, key: "API_TOKEN"},
		{name: "missing democtl API token", service: ServiceDemoctl, key: "API_TOKEN"},
		{name: "missing benchmark API token", service: ServiceBenchmark, key: "API_TOKEN"},
		{name: "invalid API token", service: ServiceAPI, key: "API_TOKEN", value: planted},
		{name: "short worker signing key", service: ServiceWorker, key: "FULFILLMENT_SIGNING_KEY_V1", value: "abcd"},
		{name: "invalid worker signing key", service: ServiceWorker, key: "NOTIFICATION_SIGNING_KEY_V1", value: planted},
		{name: "missing demo control token", service: ServiceDemoctl, key: "DEMO_CONTROL_TOKEN"},
		{name: "missing benchmark control token", service: ServiceBenchmark, key: "DEMO_CONTROL_TOKEN"},
		{name: "short demo control token", service: ServiceDemoctl, key: "DEMO_CONTROL_TOKEN", value: "abcd"},
		{name: "receiver missing code", service: ServiceReceiver, key: "RECEIVER_CODE"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			values := validEnvironment()
			if test.value == "" {
				delete(values, test.key)
			} else {
				values[test.key] = test.value
			}
			_, err := LoadFrom(test.service, mapLookup(values))
			if err == nil {
				t.Fatal("LoadFrom accepted invalid or missing secret")
			}
			if strings.Contains(err.Error(), planted) {
				t.Fatalf("configuration error disclosed planted value: %v", err)
			}
		})
	}
}

func TestLoadRejectsInvalidTimingAndConcurrency(t *testing.T) {
	cases := []struct {
		name   string
		values map[string]string
	}{
		{name: "zero concurrency", values: map[string]string{"WORKER_CONCURRENCY": "0"}},
		{name: "too much concurrency", values: map[string]string{"WORKER_CONCURRENCY": "33"}},
		{name: "nonnumeric concurrency", values: map[string]string{"WORKER_CONCURRENCY": "many"}},
		{name: "invalid poll duration", values: map[string]string{"WORKER_POLL_INTERVAL": "soon"}},
		{name: "zero reaper duration", values: map[string]string{"REAPER_INTERVAL": "0s"}},
		{name: "negative database timeout", values: map[string]string{"DB_STATEMENT_TIMEOUT": "-1s"}},
		{name: "zero lease margin", values: map[string]string{"LEASE_MARGIN": "0s"}},
		{name: "timeout and margin consume lease", values: map[string]string{"HTTP_TIMEOUT": "20s", "LEASE_MARGIN": "10s"}},
		{name: "margin leaves no HTTP time", values: map[string]string{"HTTP_TIMEOUT": "25s", "LEASE_MARGIN": "5s"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			values := validEnvironment()
			for key, value := range test.values {
				values[key] = value
			}
			if _, err := LoadFrom(ServiceSeed, mapLookup(values)); err == nil {
				t.Fatal("LoadFrom accepted invalid timing or concurrency")
			}
		})
	}

	values := validEnvironment()
	values["LEASE_MARGIN"] = "2s"
	settings, err := LoadFrom(ServiceSeed, mapLookup(values))
	if err != nil {
		t.Fatal("load positive configured lease margin:", err)
	}
	if settings.LeaseMargin() != 2*time.Second {
		t.Fatalf("lease margin = %s, want 2s", settings.LeaseMargin())
	}
}

func TestRegistryForModeUsesFixedDestinations(t *testing.T) {
	cases := []struct {
		mode            RunMode
		fulfillmentURL  string
		notificationURL string
	}{
		{mode: RunModeCompose, fulfillmentURL: "http://fulfillment:8081/webhook", notificationURL: "http://notification:8081/webhook"},
		{mode: RunModeHost, fulfillmentURL: "http://127.0.0.1:8082/webhook", notificationURL: "http://127.0.0.1:8083/webhook"},
	}
	for _, test := range cases {
		t.Run(string(test.mode), func(t *testing.T) {
			registry, err := RegistryForMode(test.mode)
			if err != nil {
				t.Fatal(err)
			}
			if tenant := registry.Tenant(); tenant.ID != TenantID || tenant.Name != "Webhook Delivery Demo" {
				t.Fatalf("tenant = %+v", tenant)
			}
			destinations := registry.Destinations()
			if len(destinations) != 2 {
				t.Fatalf("destination count = %d, want 2", len(destinations))
			}
			if destinations[0] != (Destination{ID: FulfillmentDestination, TenantID: TenantID, Code: "fulfillment", URL: test.fulfillmentURL, KeyRef: FulfillmentKeyRef}) {
				t.Fatalf("fulfillment destination = %+v", destinations[0])
			}
			if destinations[1] != (Destination{ID: NotificationDestination, TenantID: TenantID, Code: "notification", URL: test.notificationURL, KeyRef: NotificationKeyRef}) {
				t.Fatalf("notification destination = %+v", destinations[1])
			}
			destinations[0].URL = "http://attacker.invalid/"
			if registry.Destinations()[0].URL != test.fulfillmentURL {
				t.Fatal("mutating returned destinations changed the registry")
			}
		})
	}
	if _, err := RegistryForMode("arbitrary"); err == nil {
		t.Fatal("registry accepted an arbitrary run mode")
	}
}

func validEnvironment() map[string]string {
	return map[string]string{
		"DATABASE_URL":                "postgres://user:password@127.0.0.1:5432/webhook_dev?sslmode=disable",
		"RUN_MODE":                    "compose",
		"API_TOKEN":                   validTestSecret,
		"DEMO_CONTROL_TOKEN":          validTestSecret,
		"FULFILLMENT_SIGNING_KEY_V1":  validTestSecret,
		"NOTIFICATION_SIGNING_KEY_V1": validTestSecret,
		"RECEIVER_CODE":               "fulfillment",
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestSigningKeyLookupIsolationAndCopies(t *testing.T) {
	for _, code := range []string{"fulfillment", "notification"} {
		values := validEnvironment()
		values["RECEIVER_CODE"] = code
		settings, err := LoadFrom(ServiceReceiver, mapLookup(values))
		if err != nil {
			t.Fatal(err)
		}
		ref, other := FulfillmentKeyRef, NotificationKeyRef
		if code == "notification" {
			ref, other = other, ref
		}
		key, ok := settings.SigningKey(ref)
		if !ok || len(key) != 32 {
			t.Fatal("missing receiver key")
		}
		key[0] ^= 0xff
		again, _ := settings.SigningKey(ref)
		if key[0] == again[0] {
			t.Fatal("lookup exposes mutable key")
		}
		if _, ok := settings.SigningKey(other); ok {
			t.Fatal("receiver loaded another destination key")
		}
		if _, ok := settings.SigningKey("unknown"); ok {
			t.Fatal("unknown key accepted")
		}
	}
	values := validEnvironment()
	values["FULFILLMENT_SIGNING_KEY_V1"] = strings.Repeat("ab", 33)
	if _, err := LoadFrom(ServiceWorker, mapLookup(values)); err == nil {
		t.Fatal("oversized signing key accepted")
	}
}
