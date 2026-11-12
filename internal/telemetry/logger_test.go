package telemetry

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestJSONLoggerKeepsOnlyBoundedAllowlistedAttributes(t *testing.T) {
	var output bytes.Buffer
	logger := NewJSONLogger(&output, "api")
	logger.Info("http request",
		slog.String("request_id", "00000000-0000-4000-8000-000000000001"),
		slog.String("event_id", "00000000-0000-4000-8000-000000000002"),
		slog.String("path", "/v1/events?token=planted-token"),
		slog.String("url", "https://receiver.invalid/planted-url"),
		slog.String("payload", "planted-payload"),
		slog.String("authorization", "Bearer planted-header"),
		slog.String("error", "postgres://user:planted-dsn@db/app"),
		slog.String("error_code", "attacker-error-with-token"),
		slog.String("outcome", "attacker-outcome-with-token"),
		slog.String("worker_id", "worker-id-with-token"),
	)
	got := output.String()
	for _, secret := range []string{"planted-token", "planted-url", "planted-payload", "planted-header", "planted-dsn", "attacker-error", "worker-id-with-token"} {
		if strings.Contains(got, secret) {
			t.Fatalf("logger disclosed %q: %s", secret, got)
		}
	}
	for _, field := range []string{`"request_id"`, `"event_id"`, `"error_code":"internal_error"`, `"outcome":"unknown"`, `"worker_id":"unknown"`} {
		if !strings.Contains(got, field) {
			t.Fatalf("logger omitted bounded field %s: %s", field, got)
		}
	}
	if strings.Contains(got, `"path"`) || strings.Contains(got, `"url"`) || strings.Contains(got, `"payload"`) || strings.Contains(got, `"error"`) {
		t.Fatalf("logger retained a disallowed field: %s", got)
	}
}

func TestJSONLoggerDropsUnapprovedMessageAndInvalidIdentifiers(t *testing.T) {
	var output bytes.Buffer
	NewJSONLogger(&output, "untrusted-component").Info("planted-message", slog.String("request_id", "planted-request-id"))
	got := output.String()
	if strings.Contains(got, "planted-message") || strings.Contains(got, "planted-request-id") || !strings.Contains(got, "redacted log event") || !strings.Contains(got, `"component":"service"`) {
		t.Fatalf("logger did not redact unapproved values: %s", got)
	}
}
