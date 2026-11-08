package signing

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// PUBLIC TEST-ONLY key: bytes 00 through 1f. Never use for local runtime.
const operation = "00000000-0000-4000-8000-000000000101"
const body = `{"data":{"message":"नमस्ते 🌍"}}`
const expected = "v1=8842482d13bc5517d907913f8c2da95e6b81e055b501051bb18954cf14de2ca7"

type keys map[string][]byte

func (k keys) SigningKey(ref string) ([]byte, bool) { v, ok := k[ref]; return v, ok }
func fixture() (Auth, http.Header) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	a := Auth{Keys: keys{"public-test-v1": key}, Now: func() time.Time { return time.Unix(1700000000, 0) }}
	h := http.Header{"Idempotency-Key": {operation}, "Webhook-Id": {operation}, "Webhook-Timestamp": {"1700000000"}, "Webhook-Key-Id": {"public-test-v1"}, "Webhook-Signature": {expected}}
	return a, h
}
func TestIndependentPublicVector(t *testing.T) {
	a, h := fixture()
	id, err := a.Verify(h, []byte(body))
	if err != nil || id != operation {
		t.Fatalf("verify: %q %v", id, err)
	}
	signed, err := a.Sign(operation, "public-test-v1", []byte(body))
	if err != nil || signed.Get("Webhook-Signature") != expected {
		t.Fatalf("sign mismatch: %v", err)
	}
}
func TestRejectInvalidAuthentication(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(http.Header)
		body   string
	}{
		{"body", func(http.Header) {}, body + " "},
		{"timestamp changed", func(h http.Header) { h.Set("Webhook-Timestamp", "1700000001") }, body},
		{"operation changed", func(h http.Header) {
			h.Set("Webhook-Id", "00000000-0000-4000-8000-000000000102")
			h.Set("Idempotency-Key", h.Get("Webhook-Id"))
		}, body},
		{"unknown ref", func(h http.Header) { h.Set("Webhook-Key-Id", "unknown") }, body},
		{"mismatched IDs", func(h http.Header) { h.Set("Idempotency-Key", "00000000-0000-4000-8000-000000000102") }, body},
		{"UUID misplaced dash", func(h http.Header) { h.Set("Webhook-Id", "00000000-0000-4000-8000-00000000010-") }, body},
		{"uppercase hex", func(h http.Header) { h.Set("Webhook-Signature", strings.ToUpper(expected)) }, body},
		{"bad hex", func(h http.Header) { h.Set("Webhook-Signature", "v1="+strings.Repeat("z", 64)) }, body},
		{"wrong digest", func(h http.Header) { h.Set("Webhook-Signature", "v1="+strings.Repeat("0", 64)) }, body},
		{"short digest", func(h http.Header) { h.Set("Webhook-Signature", "v1=ab") }, body},
		{"combined signature", func(h http.Header) { h.Set("Webhook-Signature", expected+","+expected) }, body},
	}
	for _, name := range []string{"Idempotency-Key", "Webhook-Id", "Webhook-Timestamp", "Webhook-Key-Id", "Webhook-Signature"} {
		name := name
		cases = append(cases, struct {
			name   string
			mutate func(http.Header)
			body   string
		}{"missing " + name, func(h http.Header) { h.Del(name) }, body}, struct {
			name   string
			mutate func(http.Header)
			body   string
		}{"repeated " + name, func(h http.Header) { h.Add(name, h.Get(name)) }, body}, struct {
			name   string
			mutate func(http.Header)
			body   string
		}{"case duplicate " + name, func(h http.Header) { h[strings.ToLower(name)] = []string{h.Get(name)} }, body}, struct {
			name   string
			mutate func(http.Header)
			body   string
		}{"overlong " + name, func(h http.Header) { h.Set(name, strings.Repeat("x", 1024)) }, body})
	}
	for _, stamp := range []string{"", "-1", "+1700000000", "01700000000", "1700000000 ", "1e9", "9223372036854775808", strings.Repeat("9", 100), "1699999699", "1700000301"} {
		stamp := stamp
		cases = append(cases, struct {
			name   string
			mutate func(http.Header)
			body   string
		}{"timestamp " + stamp, func(h http.Header) { h.Set("Webhook-Timestamp", stamp) }, body})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, h := fixture()
			tc.mutate(h)
			id, err := a.Verify(h, []byte(tc.body))
			if err != ErrInvalid || id != "" {
				t.Fatalf("wanted generic failure, got %q %v", id, err)
			}
			if err.Error() != "invalid webhook authentication" {
				t.Fatal("error disclosure")
			}
		})
	}
}
func TestToleranceAndFreshReplay(t *testing.T) {
	for _, delta := range []int64{-301, -300, 0, 300, 301} {
		t.Run(strconv.FormatInt(delta, 10), func(t *testing.T) {
			a, _ := fixture()
			h, err := a.Sign(operation, "public-test-v1", []byte(body))
			if err != nil {
				t.Fatal(err)
			}
			a.Now = func() time.Time { return time.Unix(1700000000+delta, 0) }
			_, err = a.Verify(h, []byte(body))
			if (err == nil) != (delta >= -300 && delta <= 300) {
				t.Fatalf("delta %d: %v", delta, err)
			}
		})
	}
	a, old := fixture()
	a.Now = func() time.Time { return time.Unix(1700001000, 0) }
	fresh, err := a.Sign(operation, "public-test-v1", []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Get("Webhook-Timestamp") == old.Get("Webhook-Timestamp") || fresh.Get("Webhook-Signature") == expected {
		t.Fatal("reused signature")
	}
	if _, err = a.Verify(old, []byte(body)); err != ErrInvalid {
		t.Fatal("old accepted")
	}
	if id, err := a.Verify(fresh, []byte(body)); err != nil || id != operation {
		t.Fatal("fresh replay failed", err)
	}
	fresh.Set("X-Unrelated", "allowed")
	if _, err = a.Verify(fresh, []byte(body)); err != nil {
		t.Fatal("unrelated header rejected")
	}
}
func TestOverflowSafeTolerance(t *testing.T) {
	for _, tc := range []struct {
		now, stamp int64
		want       bool
	}{{math.MinInt64, math.MaxInt64, false}, {math.MaxInt64, math.MinInt64, false}, {math.MaxInt64, math.MaxInt64 - 300, true}, {math.MinInt64, math.MinInt64 + 300, true}, {math.MinInt64, 0, false}, {0, math.MaxInt64, false}} {
		if within(tc.now, tc.stamp) != tc.want {
			t.Fatalf("bounds %+v", tc)
		}
	}
}
func TestInvalidSignerConfiguration(t *testing.T) {
	a, _ := fixture()
	for _, tc := range []struct{ id, ref string }{{"bad", "public-test-v1"}, {operation, "unknown"}, {operation, "bad ref"}} {
		if _, err := a.Sign(tc.id, tc.ref, []byte(body)); err != ErrInvalid {
			t.Fatal("invalid signing accepted")
		}
	}
	a.Keys = nil
	if _, err := a.Sign(operation, "public-test-v1", nil); err != ErrInvalid {
		t.Fatal("nil keys accepted")
	}
	a.Keys = keys{"public-test-v1": {1}}
	if _, err := a.Sign(operation, "public-test-v1", nil); err != ErrInvalid {
		t.Fatal("short key accepted")
	}
}

func TestChangedKnownKeyAndUnusableVerifierKeys(t *testing.T) {
	a, h := fixture()
	other := make([]byte, 32)
	other[0] = 255
	a.Keys.(keys)["other-public-test-v1"] = other
	h.Set("Webhook-Key-Id", "other-public-test-v1")
	if _, err := a.Verify(h, []byte(body)); err != ErrInvalid {
		t.Fatal("changed key ID accepted")
	}
	a, h = fixture()
	a.Keys = nil
	if _, err := a.Verify(h, []byte(body)); err != ErrInvalid {
		t.Fatal("nil verifier keys accepted")
	}
	a.Keys = keys{"public-test-v1": {1}}
	if _, err := a.Verify(h, []byte(body)); err != ErrInvalid {
		t.Fatal("short verifier key accepted")
	}
}
