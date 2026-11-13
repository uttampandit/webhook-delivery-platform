package democtl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

const testToken = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
const eventID = "00000000-0000-4000-8000-000000000101"
const deliveryID = "00000000-0000-4000-8000-000000000102"

func settings(t *testing.T) config.Settings {
	t.Helper()
	values := map[string]string{"DATABASE_URL": "postgres://test:test@127.0.0.1:5432/webhook_dev?sslmode=disable", "RUN_MODE": "host", "API_TOKEN": testToken, "DEMO_CONTROL_TOKEN": testToken}
	s, e := config.LoadFrom(config.ServiceDemoctl, func(k string) (string, bool) { v, ok := values[k]; return v, ok })
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func invoke(t *testing.T, c *Client, want int, args ...string) string {
	t.Helper()
	var out, errs bytes.Buffer
	code := c.Run(context.Background(), args, &out, &errs)
	if strings.Contains(out.String()+errs.String(), testToken) {
		t.Fatal("secret leaked")
	}
	if code != want {
		t.Fatalf("exit=%d want=%d output=%s error=%s", code, want, out.String(), errs.String())
	}
	if want != 0 {
		return errs.String()
	}
	return out.String()
}
func TestPublishPreservesBytesAndKeyAfterResponseLoss(t *testing.T) {
	var bodies, keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Error("missing authentication")
		}
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		if len(bodies) == 1 {
			conn, _, e := w.(http.Hijacker).Hijack()
			if e != nil {
				t.Error(e)
				return
			}
			conn.Close()
			return
		}
		fmt.Fprintf(w, `{"event_id":%q,"delivery_ids":[%q],"secret":%q}`, eventID, deliveryID, testToken)
	}))
	defer server.Close()
	c := New(settings(t), Endpoints{API: server.URL})
	args := []string{"publish", "--scenario", "fresh_123", "--order", "order-1", "--destinations", "fulfillment"}
	invoke(t, c, 1, args...)
	invoke(t, c, 0, args...)
	if len(bodies) != 2 || bodies[0] != bodies[1] || keys[0] != keys[1] || keys[0] == "" {
		t.Fatal("retry changed request or transport retried silently")
	}
	raw := `{ "type":"order.confirmed", "payload":{}, "destination_ids":["` + config.FulfillmentDestination + `"] }`
	file := filepath.Join(t.TempDir(), "request.json")
	if e := os.WriteFile(file, []byte(raw), 0600); e != nil {
		t.Fatal(e)
	}
	invoke(t, c, 0, "publish", "--file", file, "--key", "exact-request")
	if bodies[2] != raw || keys[2] != "exact-request" {
		t.Fatal("file bytes/key changed")
	}
}
func TestStrictFlagsAndSanitizedFailures(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(500); fmt.Fprint(w, testToken) }))
	defer server.Close()
	c := New(settings(t), Endpoints{API: server.URL})
	for _, args := range [][]string{{"publish", "--token", testToken}, {"publish", "--scenario", "a", "--scenario", "b"}, {"replay", "--delivery", deliveryID}, {"wait", "--event", eventID, "--state", "succeeded", "--timeout", "0s"}, {"scenario", "create", "--id", "ok", "--receiver", "external"}, {"publish", "positional"}, {"run", "--token", testToken}} {
		invoke(t, c, 1, args...)
	}
	if calls != 0 {
		t.Fatal("invalid flags issued HTTP")
	}
	err := invoke(t, c, 1, "replay", "--delivery", deliveryID, "--key", "retry-1")
	if !strings.Contains(err, "status 500") {
		t.Fatal(err)
	}
}

func TestRunFailureWritesSanitizedPartialReport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, testToken)
	}))
	defer server.Close()
	c := New(settings(t), Endpoints{
		API:      server.URL,
		Controls: map[string]string{"fulfillment": server.URL, "notification": server.URL},
	})
	var out, errs bytes.Buffer
	if code := c.Run(context.Background(), []string{"run"}, &out, &errs); code != 1 {
		t.Fatalf("run exit=%d, want 1", code)
	}
	var report runReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("failure report missing: %v (%q)", err, out.String())
	}
	if report.Status != "failed" || report.Failure == nil || report.Failure.Step != "healthy_fanout" || len(report.Scenarios) != 1 || report.Scenarios[0].Status != "failed" {
		t.Fatalf("unexpected partial report: %+v", report)
	}
	if strings.Contains(out.String()+errs.String(), testToken) {
		t.Fatal("failed demo report leaked a credential")
	}
}

func TestWaitDeadlineRetainsOnlyValidatedStatuses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"id":%q,"deliveries":[{"id":%q,"destination_code":"fulfillment","status":"pending"}],"payload":%q}`, eventID, deliveryID, testToken)
	}))
	defer server.Close()
	c := New(settings(t), Endpoints{API: server.URL})
	c.PollInterval = time.Millisecond
	started := time.Now()
	err := invoke(t, c, 1, "wait", "--event", eventID, "--state", "dead", "--timeout", "40ms")
	if !strings.Contains(err, "last: fulfillment=pending") || time.Since(started) > time.Second {
		t.Fatal(err)
	}
}
