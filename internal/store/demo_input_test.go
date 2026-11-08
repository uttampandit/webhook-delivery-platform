package store

import (
	"strings"
	"testing"
)

const demoBody = `{"event_id":"00000000-0000-4000-8000-000000000101","type":"order.confirmed","occurred_at":"2026-11-08T00:00:00Z","data":{"scenario_id":"demo_1","order_id":"order-1","quantity":2}}`

func TestValidateDemoEnvelope(t *testing.T) {
	valid := []string{demoBody, strings.Replace(demoBody, `"quantity":2`, `"quantity":100000,"padding":{"ignored":true}`, 1), strings.Replace(demoBody, "order-1", "नमस्ते 🌍", 1)}
	for _, raw := range valid {
		e, err := ValidateDemoEnvelope([]byte(raw))
		if err != nil || e.ScenarioID() != "demo_1" {
			t.Fatal("valid envelope rejected", err)
		}
	}
	invalid := []string{"null", "[]", demoBody + " {}", strings.Replace(demoBody, `"quantity":2`, `"quantity":null`, 1), strings.Replace(demoBody, `"quantity":2`, `"quantity":1.5`, 1), strings.Replace(demoBody, `"quantity":2`, `"quantity":0`, 1), strings.Replace(demoBody, `"quantity":2`, `"quantity":100001`, 1), strings.Replace(demoBody, "order-1", strings.Repeat("x", 129), 1), strings.Replace(demoBody, "demo_1", "bad id", 1), strings.Replace(demoBody, "2026-11-08T00:00:00Z", "invalid", 1), strings.Replace(demoBody, "00000000-0000-4000-8000-000000000101", "bad", 1), strings.Replace(demoBody, `"data":`, `"extra":1,"data":`, 1), strings.Replace(demoBody, `"quantity":2`, `"quantity":2,"quantity":2`, 1), strings.Replace(demoBody, `"quantity":2`, `"quantity":2,"padding":{"a":1,"a":2}`, 1), strings.Replace(demoBody, `"data":{`, `"data":null,"unused":{`, 1)}
	for _, raw := range invalid {
		if _, err := ValidateDemoEnvelope([]byte(raw)); err == nil {
			t.Fatalf("invalid envelope accepted: %.100s", raw)
		}
	}
	if _, err := ValidateDemoEnvelope([]byte(strings.Replace(demoBody, "order.confirmed", "other.event", 1))); err != ErrDemoUnsupported {
		t.Fatal("unsupported type not rejected", err)
	}
}
