package telemetry

import (
	"io"
	"log/slog"
	"os"
	"regexp"
)

var safeID = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$`)

func ConfigureJSONLogger(component string) {
	slog.SetDefault(NewJSONLogger(os.Stderr, component))
}

func NewJSONLogger(writer io.Writer, component string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{ReplaceAttr: redactLogAttr})).With(
		slog.String("component", boundedComponent(component)),
	)
}

func redactLogAttr(groups []string, attr slog.Attr) slog.Attr {
	if len(groups) > 0 {
		return slog.Attr{}
	}
	if attr.Value.Kind() == slog.KindGroup {
		return slog.Attr{}
	}
	switch attr.Key {
	case slog.TimeKey, slog.LevelKey:
		return attr
	case slog.MessageKey:
		if _, ok := allowedMessages[attr.Value.String()]; !ok {
			return slog.String(slog.MessageKey, "redacted log event")
		}
		return attr
	case "component":
		return slog.String(attr.Key, boundedComponent(attr.Value.String()))
	case "request_id", "event_id", "delivery_id", "attempt_id", "worker_id":
		if attr.Value.Kind() != slog.KindString {
			return slog.Attr{}
		}
		value := attr.Value.String()
		if attr.Key == "worker_id" {
			return slog.String(attr.Key, boundedWorkerID(value))
		}
		if !safeID.MatchString(value) {
			return slog.Attr{}
		}
		return attr
	case "cycle_no", "attempt_no", "generation", "http_status", "duration_ms", "scheduled_delay_ms":
		if !isInteger(attr.Value.Kind()) {
			return slog.Attr{}
		}
		return attr
	case "outcome":
		if attr.Value.Kind() != slog.KindString {
			return slog.String(attr.Key, "unknown")
		}
		if _, ok := allowedOutcomes[attr.Value.String()]; !ok {
			return slog.String(attr.Key, "unknown")
		}
		return attr
	case "error_code":
		if attr.Value.Kind() != slog.KindString {
			return slog.String(attr.Key, "internal_error")
		}
		if _, ok := allowedErrorCodes[attr.Value.String()]; !ok {
			return slog.String(attr.Key, "internal_error")
		}
		return attr
	case "error":
		return slog.Attr{}
	default:
		return slog.Attr{}
	}
}

func isInteger(kind slog.Kind) bool {
	switch kind {
	case slog.KindInt64, slog.KindUint64:
		return true
	default:
		return false
	}
}

func boundedWorkerID(value string) string {
	if _, ok := allowedWorkerIDs[value]; !ok {
		return "unknown"
	}
	return value
}

func boundedComponent(component string) string {
	if _, ok := allowedComponents[component]; !ok {
		return "service"
	}
	return component
}

var allowedComponents = map[string]struct{}{
	"api": {}, "worker": {}, "receiver": {}, "seed": {}, "migrate": {},
}

var allowedWorkerIDs = map[string]struct{}{"worker": {}}

var allowedOutcomes = map[string]struct{}{
	"acknowledged": {}, "http_failure": {}, "unconfirmed": {}, "local_failure": {},
	"accepted": {}, "deduplicated": {}, "conflict": {}, "invalid": {}, "error": {},
}

var allowedErrorCodes = map[string]struct{}{
	"api_startup_failed": {}, "worker_startup_failed": {}, "receiver_startup_failed": {},
	"metrics_listener_failed": {},
	"seed_failed":             {}, "migration_failed": {}, "claim_failed": {}, "claim_batch_overflow": {},
	"finalization_failed": {}, "stale_finalization": {}, "recovery_failed": {},
	"missing_signing_key": {}, "invalid_destination_configuration": {},
	"hijack_failed": {}, "close_failed": {}, "internal_error": {},
}

var allowedMessages = map[string]struct{}{
	"http request": {}, "stale finalization rejected": {}, "delivery local configuration failure": {},
	"committed response drop failed": {}, "committed response close failed": {},
	"worker claim failed": {}, "worker rejected claim batch exceeding free slots": {},
	"worker finalization failed": {}, "lease recovery failed": {},
	"api stopped": {}, "worker stopped": {}, "receiver stopped": {},
	"seed stopped": {}, "seed complete": {}, "migrate stopped": {},
}
