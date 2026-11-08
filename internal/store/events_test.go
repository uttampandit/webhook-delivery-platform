package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"
)

const validEventRequestBody = `{"type":"order.confirmed","payload":{"order_id":"o<&","quantity":1},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`

func TestValidateEventRequest(t *testing.T) {
	raw := []byte(validEventRequestBody)
	request, err := ValidateEventRequest(raw)
	if err != nil {
		t.Fatal("validate request:", err)
	}
	wantHash := sha256.Sum256(raw)
	if request.requestHash != wantHash || !bytes.Equal(request.rawBody, raw) {
		t.Fatal("validated request did not retain the exact original bytes and hash")
	}
	if request.eventType != "order.confirmed" || !bytes.Equal(request.payload, []byte(`{"order_id":"o<&","quantity":1}`)) {
		t.Fatalf("parsed event data = (%q, %s)", request.eventType, request.payload)
	}

	raw[0] = ' '
	if !bytes.Equal(request.rawBody, []byte(validEventRequestBody)) {
		t.Fatal("validated request retained caller-owned mutable bytes")
	}
}

func TestValidateEventRequestRejectsInvalidJSONAndShape(t *testing.T) {
	depth33 := `{"type":"order.confirmed","payload":{"a":` + strings.Repeat("[", 32) + `0` + strings.Repeat("]", 32) + `},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`
	invalidUTF8 := append([]byte(`{"type":"order.confirmed","payload":{"value":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`)...)
	cases := []struct {
		name string
		body []byte
		want error
	}{
		{name: "unknown top-level field", body: []byte(`{"type":"order.confirmed","payload":{},"destination_ids":["00000000-0000-4000-8000-000000000101"],"extra":true}`), want: ErrInvalidInput},
		{name: "duplicate top-level key", body: []byte(`{"type":"order.confirmed","\u0074ype":"other","payload":{},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`), want: ErrInvalidJSON},
		{name: "duplicate nested key", body: []byte(`{"type":"order.confirmed","payload":{"a":1,"a":2},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`), want: ErrInvalidJSON},
		{name: "trailing JSON", body: []byte(validEventRequestBody + ` {}`), want: ErrInvalidJSON},
		{name: "invalid UTF-8", body: invalidUTF8, want: ErrInvalidJSON},
		{name: "malformed JSON", body: []byte(`{"type":`), want: ErrInvalidJSON},
		{name: "empty body", body: nil, want: ErrInvalidJSON},
		{name: "null payload", body: []byte(`{"type":"order.confirmed","payload":null,"destination_ids":["00000000-0000-4000-8000-000000000101"]}`), want: ErrInvalidInput},
		{name: "array payload", body: []byte(`{"type":"order.confirmed","payload":[],"destination_ids":["00000000-0000-4000-8000-000000000101"]}`), want: ErrInvalidInput},
		{name: "invalid event type", body: []byte(`{"type":"Order.confirmed","payload":{},"destination_ids":["00000000-0000-4000-8000-000000000101"]}`), want: ErrInvalidInput},
		{name: "uppercase uuid", body: []byte(`{"type":"order.confirmed","payload":{},"destination_ids":["00000000-0000-4000-8000-00000000010A"]}`), want: ErrInvalidInput},
		{name: "duplicate destination", body: []byte(`{"type":"order.confirmed","payload":{},"destination_ids":["00000000-0000-4000-8000-000000000101","00000000-0000-4000-8000-000000000101"]}`), want: ErrInvalidInput},
		{name: "depth over limit", body: []byte(depth33), want: ErrInvalidJSON},
		{name: "oversized raw body", body: bytes.Repeat([]byte(" "), MaxEventRequestBytes+1), want: ErrInvalidInput},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ValidateEventRequest(test.body); !errors.Is(err, test.want) {
				t.Fatalf("ValidateEventRequest error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	for _, test := range []struct {
		name string
		key  string
		ok   bool
	}{
		{name: "normal", key: "order-123", ok: true},
		{name: "maximum", key: strings.Repeat("x", 128), ok: true},
		{name: "empty", key: ""},
		{name: "space", key: "order 123"},
		{name: "newline", key: "order\n123"},
		{name: "non ASCII", key: "订单-123"},
		{name: "too long", key: strings.Repeat("x", 129)},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateIdempotencyKey(test.key)
			if test.ok && err != nil {
				t.Fatalf("valid key rejected: %v", err)
			}
			if !test.ok && !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("invalid key error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestMarshalOutgoingEventDoesNotHTMLExpandPayload(t *testing.T) {
	payload := []byte(`{"html":"<>&","unicode":"雪🙂"}`)
	first, err := marshalOutgoingEvent("00000000-0000-4000-8000-000000000001", "order.confirmed", mustParseTime(t, "2026-11-06T01:02:03.000000004Z"), payload)
	if err != nil {
		t.Fatal("marshal outgoing event:", err)
	}
	second, err := marshalOutgoingEvent("00000000-0000-4000-8000-000000000001", "order.confirmed", mustParseTime(t, "2026-11-06T01:02:03.000000004Z"), payload)
	if err != nil {
		t.Fatal("marshal outgoing event again:", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("outgoing serialization was not stable")
	}
	if bytes.Contains(first, []byte(`\u003c`)) || bytes.Contains(first, []byte(`\u0026`)) || !bytes.Contains(first, []byte(`<>&`)) || !bytes.Contains(first, []byte("雪🙂")) {
		t.Fatalf("outgoing body HTML-escaped or lost Unicode: %s", first)
	}
	if len(first) > maxEventWireBytes {
		t.Fatalf("outgoing body size = %d; over %d-byte limit", len(first), maxEventWireBytes)
	}
}

func mustParseTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal("parse test timestamp:", err)
	}
	return parsed
}

func TestEventJSONNumbersPreserveLexemes(t *testing.T) {
	for _, number := range []string{"1e1000", "-1e1000", "1e-1000", "9007199254740993"} {
		t.Run(number, func(t *testing.T) {
			raw := []byte(strings.Replace(validEventRequestBody, `"quantity":1`, `"quantity":1,"extra":`+number, 1))
			request, err := ValidateEventRequest(raw)
			if err != nil {
				t.Fatalf("valid JSON number rejected: %v", err)
			}
			if !bytes.Equal(request.rawBody, raw) {
				t.Fatal("raw body changed")
			}
			wire, err := marshalOutgoingEvent("00000000-0000-4000-8000-000000000001", request.eventType, time.Now(), request.payload)
			if err != nil || !bytes.Contains(wire, []byte(`"extra":`+number)) {
				t.Fatalf("number changed in wire body: %s, %v", wire, err)
			}
			envelope := strings.Replace(demoBody, `"quantity":2`, `"quantity":2,"extra":`+number, 1)
			if _, err := ValidateDemoEnvelope([]byte(envelope)); err != nil {
				t.Fatalf("ignored receiver data rejected: %v", err)
			}
		})
	}
}
