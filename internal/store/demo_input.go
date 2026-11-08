package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"
)

const MaxDemoBodyBytes = 96 * 1024

var (
	ErrDemoNotFound    = errors.New("scenario not found")
	ErrDemoConflict    = errors.New("scenario or operation conflicts")
	ErrDemoUnsupported = errors.New("unsupported receiver mode or event type")
	ErrDemoUnavailable = errors.New("receiver store unavailable")
)

// DemoEnvelope can only be created by validation of the exact signed bytes.
type DemoEnvelope struct {
	valid           bool
	scenario, order string
	quantity        int
	hash            [32]byte
}

func (e DemoEnvelope) ScenarioID() string { return e.scenario }

// ValidateDemoJSON rejects duplicate keys at every depth and trailing values.
func ValidateDemoJSON(raw []byte) error {
	if len(raw) == 0 || !utf8.Valid(raw) || rejectDuplicateJSONKeys(raw) != nil {
		return ErrInvalidJSON
	}
	return nil
}
func ValidateDemoEnvelope(raw []byte) (DemoEnvelope, error) {
	invalid := func() (DemoEnvelope, error) { return DemoEnvelope{}, ErrInvalidInput }
	if len(raw) > MaxDemoBodyBytes || ValidateDemoJSON(raw) != nil {
		return invalid()
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 4 {
		return invalid()
	}
	for _, name := range []string{"event_id", "type", "occurred_at", "data"} {
		if _, ok := fields[name]; !ok {
			return invalid()
		}
	}
	var id, kind, stamp string
	if json.Unmarshal(fields["event_id"], &id) != nil || !isCanonicalUUID(id) || json.Unmarshal(fields["type"], &kind) != nil || kind == "" || json.Unmarshal(fields["occurred_at"], &stamp) != nil {
		return invalid()
	}
	if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
		return invalid()
	}
	var data map[string]json.RawMessage
	if json.Unmarshal(fields["data"], &data) != nil || data == nil {
		return invalid()
	}
	if kind != "order.confirmed" {
		return DemoEnvelope{}, ErrDemoUnsupported
	}
	var scenario, order string
	var quantity int
	if json.Unmarshal(data["scenario_id"], &scenario) != nil || !ValidScenarioID(scenario) || json.Unmarshal(data["order_id"], &order) != nil || utf8.RuneCountInString(order) < 1 || utf8.RuneCountInString(order) > 128 || json.Unmarshal(data["quantity"], &quantity) != nil || quantity < 1 || quantity > 100000 {
		return invalid()
	}
	return DemoEnvelope{true, scenario, order, quantity, sha256.Sum256(raw)}, nil
}
func ValidScenarioID(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func sameDemoHash(a []byte, b [32]byte) bool { return bytes.Equal(a, b[:]) }
