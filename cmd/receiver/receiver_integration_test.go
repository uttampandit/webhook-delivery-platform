//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

// All credentials here are PUBLIC TEST-ONLY; these tests use isolated databases.
const controlToken = "abababababababababababababababababababababababababababababababab"
const signingSecret = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
const op = "00000000-0000-4000-8000-000000000101"

type testKeys struct{}

func (testKeys) SigningKey(ref string) ([]byte, bool) {
	key, _ := hex.DecodeString(signingSecret)
	return key, ref == "fulfillment-v1"
}
func TestReceiverHelper(t *testing.T) {
	if os.Getenv("WEBHOOK_RECEIVER_TEST_PROCESS") != "1" {
		return
	}
	if run() != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
func TestReceiverHTTPPostgresAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Apply(ctx, db.Pool, dir); err != nil {
		t.Fatal(err)
	}
	// ConnString retains the original maintenance DSN even when the pool config's
	// Database field was changed. Build the child URL explicitly for this test DB.
	childURL, err := url.Parse(db.AdminConnectionString())
	if err != nil || childURL.Scheme != "postgres" && childURL.Scheme != "postgresql" {
		t.Fatal("test administrator must use a PostgreSQL URL")
	}
	childURL.Path = "/" + db.Name()
	childURL.RawPath = ""
	q := childURL.Query()
	q.Del("dbname")
	childURL.RawQuery = q.Encode()
	for _, addr := range []string{"127.0.0.1:8082", "127.0.0.1:9092"} {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal("receiver test port already in use")
		}
		listener.Close()
	}
	client := &http.Client{Timeout: 3 * time.Second}
	send := func(method, url, body, token string, signed bool) int {
		req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if signed {
			req.Header, err = (signing.Auth{Keys: testKeys{}}).Sign(op, "fulfillment-v1", []byte(body))
			if err != nil {
				t.Fatal(err)
			}
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal("HTTP request failed:", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	start := func() func() {
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, exe, "-test.run=^TestReceiverHelper$")
		// Keep the parent environment but replace the values owned by this child.
		overrides := map[string]string{"WEBHOOK_RECEIVER_TEST_PROCESS": "1", "DATABASE_URL": childURL.String(), "RUN_MODE": "host", "RECEIVER_CODE": "fulfillment", "DEMO_CONTROL_TOKEN": controlToken, "FULFILLMENT_SIGNING_KEY_V1": signingSecret}
		for _, entry := range os.Environ() {
			name, _, _ := strings.Cut(entry, "=")
			if _, ok := overrides[name]; !ok {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		for k, v := range overrides {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		if err = cmd.Start(); err != nil {
			t.Fatal("start receiver", err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		var once sync.Once
		stop := func() {
			once.Do(func() {
				cmd.Process.Signal(syscall.SIGTERM)
				select {
				case err := <-done:
					if err != nil {
						t.Error("receiver did not stop cleanly")
					}
				case <-time.After(6 * time.Second):
					cmd.Process.Kill()
					<-done
					t.Error("receiver shutdown timed out")
				}
			})
		}
		t.Cleanup(stop)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		for {
			req, _ := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:9092/health/ready", nil)
			req.Header.Set("Authorization", "Bearer "+controlToken)
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					return stop
				}
			}
			select {
			case <-ticker.C:
			case <-done:
				once.Do(func() {})
				t.Fatal("receiver exited before readiness")
			case <-deadline.C:
				t.Fatal("receiver readiness timed out")
			}
		}
	}
	stop := start()
	control := "http://127.0.0.1:9092"
	webhook := "http://127.0.0.1:8082"
	check := func(got, want int) {
		t.Helper()
		if got != want {
			t.Fatalf("status %d want %d", got, want)
		}
	}
	create := func(id string, dedupe bool) string {
		return fmt.Sprintf(`{"scenario_id":%q,"mode":"normal","dedupe_enabled":%t}`, id, dedupe)
	}
	body := func(id string) string {
		return fmt.Sprintf(`{"event_id":"00000000-0000-4000-8000-000000000201","type":"order.confirmed","occurred_at":"2026-11-08T00:00:00Z","data":{"scenario_id":%q,"order_id":"order-1","quantity":2}}`, id)
	}
	inspect := func(id string) store.DemoScenario {
		req, _ := http.NewRequestWithContext(ctx, "GET", control+"/control/scenarios/"+id, nil)
		req.Header.Set("Authorization", "Bearer "+controlToken)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatal("inspect failed", resp.StatusCode)
		}
		var out store.DemoScenario
		if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	counts := func(id string, requests, effects int64) {
		t.Helper()
		out := inspect(id)
		if out.RequestCount != requests || out.EffectCount != effects {
			t.Fatalf("counts %+v want %d/%d", out, requests, effects)
		}
	}
	check(send("POST", control+"/control/scenarios", create("on", true), "", false), 401)
	check(send("POST", control+"/control/scenarios", create("on", true), "wrong", false), 401)
	check(send("POST", webhook+"/control/scenarios", create("on", true), controlToken, false), 404)
	check(send("POST", control+"/webhook", body("on"), controlToken, true), 404)
	check(send("GET", control+"/health/ready", "", "", false), 401)
	check(send("POST", control+"/control/scenarios", create("on", true), controlToken, false), 201)
	check(send("POST", control+"/control/scenarios", create("on", true), controlToken, false), 200)
	check(send("POST", control+"/control/scenarios", create("on", false), controlToken, false), 409)
	check(send("PATCH", control+"/control/scenarios/on", `{"dedupe_enabled":false}`, controlToken, false), 400)
	for _, mode := range []string{"http_503", "http_429", "commit_drop_once"} {
		check(send("POST", control+"/control/scenarios", strings.Replace(create("supported_"+mode, true), "normal", mode, 1), controlToken, false), 201)
	}
	check(send("POST", webhook+"/webhook", body("missing"), "", true), 404)
	check(send("POST", webhook+"/webhook", body("on"), "", false), 401)
	invalid := []string{`{}`, strings.Replace(body("on"), `"quantity":2`, `"quantity":0`, 1), strings.Replace(body("on"), `"quantity":2`, `"quantity":2,"quantity":3`, 1), body("on") + " {}", strings.Replace(body("on"), `"data":`, `"extra":true,"data":`, 1)}
	for _, raw := range invalid {
		check(send("POST", webhook+"/webhook", raw, "", true), 400)
	}
	check(send("POST", webhook+"/webhook", strings.Replace(body("on"), "order.confirmed", "other.event", 1), "", true), 422)
	check(send("POST", webhook+"/webhook", strings.Repeat("x", store.MaxDemoBodyBytes+1), "", true), 413)
	counts("on", 0, 0)
	check(send("POST", webhook+"/webhook", body("on"), "", true), 204)
	check(send("POST", webhook+"/webhook", body("on"), "", true), 204)
	counts("on", 2, 1)
	check(send("POST", webhook+"/webhook", body("on")+" ", "", true), 409)
	counts("on", 3, 1)
	// Concurrent real HTTP clients, synchronized start (no timing sleeps).
	check(send("POST", control+"/control/scenarios", create("race", true), controlToken, false), 201)
	var wg sync.WaitGroup
	gate := make(chan struct{})
	statuses := make(chan int, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-gate; statuses <- send("POST", webhook+"/webhook", body("race"), "", true) }()
	}
	close(gate)
	wg.Wait()
	close(statuses)
	for status := range statuses {
		check(status, 204)
	}
	counts("race", 12, 1)
	check(send("POST", control+"/control/scenarios", create("off", false), controlToken, false), 201)
	for i := 0; i < 2; i++ {
		check(send("POST", webhook+"/webhook", body("off"), "", true), 204)
	}
	counts("off", 2, 2)
	var ledgerCount int
	if err = db.Pool.QueryRow(ctx, `SELECT count(*) FROM demo_dedup WHERE scenario_id='off'`).Scan(&ledgerCount); err != nil || ledgerCount != 0 {
		t.Fatal("dedupe off wrote ledger")
	}
	// Force a late request-record failure, after ledger and effect writes.
	check(send("POST", control+"/control/scenarios", create("rollback", true), controlToken, false), 201)
	_, err = db.Pool.Exec(ctx, `ALTER TABLE demo_requests ADD CONSTRAINT test_reject_request CHECK (scenario_id <> 'rollback')`)
	if err != nil {
		t.Fatal(err)
	}
	check(send("POST", webhook+"/webhook", body("rollback"), "", true), 503)
	counts("rollback", 0, 0)
	if err = db.Pool.QueryRow(ctx, `SELECT count(*) FROM demo_dedup WHERE scenario_id='rollback'`).Scan(&ledgerCount); err != nil || ledgerCount != 0 {
		t.Fatal("orphan ledger")
	}
	if _, err = db.Pool.Exec(ctx, `ALTER TABLE demo_requests DROP CONSTRAINT test_reject_request`); err != nil {
		t.Fatal(err)
	}
	check(send("POST", webhook+"/webhook", body("rollback"), "", true), 204)
	counts("rollback", 1, 1)
	// A deferred constraint trigger fails COMMIT, after all writes succeeded.
	check(send("POST", control+"/control/scenarios", create("commit_fail", true), controlToken, false), 201)
	if _, err = db.Pool.Exec(ctx, `CREATE FUNCTION test_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test commit failure'; END $$; CREATE CONSTRAINT TRIGGER test_commit_failure AFTER INSERT ON demo_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.scenario_id = 'commit_fail') EXECUTE FUNCTION test_fail_commit()`); err != nil {
		t.Fatal(err)
	}
	check(send("POST", webhook+"/webhook", body("commit_fail"), "", true), 503)
	counts("commit_fail", 0, 0)
	if err = db.Pool.QueryRow(ctx, `SELECT count(*) FROM demo_dedup WHERE scenario_id='commit_fail'`).Scan(&ledgerCount); err != nil || ledgerCount != 0 {
		t.Fatal("commit failure left orphan ledger")
	}
	if _, err = db.Pool.Exec(ctx, `DROP TRIGGER test_commit_failure ON demo_requests; DROP FUNCTION test_fail_commit()`); err != nil {
		t.Fatal(err)
	}
	check(send("POST", webhook+"/webhook", body("commit_fail"), "", true), 204)
	counts("commit_fail", 1, 1)
	// Seed synthetic request history to exercise the bounded inspection response.
	check(send("POST", control+"/control/scenarios", create("bounded", true), controlToken, false), 201)
	if _, err = db.Pool.Exec(ctx, `INSERT INTO demo_requests(receiver_code,scenario_id,operation_id,planned_outcome,business_effect_added) SELECT 'fulfillment','bounded',gen_random_uuid(),'acknowledge',false FROM generate_series(1,101)`); err != nil {
		t.Fatal(err)
	}
	bounded := inspect("bounded")
	if len(bounded.OperationCounts) != 100 || bounded.RequestCount != 101 {
		t.Fatal("operation counts not bounded")
	}
	if _, err = db.Pool.Exec(ctx, `INSERT INTO demo_scenarios(receiver_code,scenario_id,mode,dedupe_enabled) VALUES ('notification','foreign','normal',true)`); err != nil {
		t.Fatal(err)
	}
	check(send("POST", webhook+"/webhook", body("foreign"), "", true), 404)
	out := inspect("on")
	if len(out.OperationCounts) != 1 || out.OperationCounts[0].RequestCount != 3 || out.OperationCounts[0].EffectCount != 1 {
		t.Fatal("wrong operation counts")
	}
	// Actual receiver process restart, reusing the same isolated database.
	stop()
	stop = start()
	counts("on", 3, 1)
	check(send("POST", webhook+"/webhook", body("on"), "", true), 204)
	counts("on", 4, 1)
	check(send("POST", webhook+"/webhook", body("off"), "", true), 204)
	counts("off", 3, 3)

	// Real receiver process restart must not rearm an already consumed drop.
	check(send("POST", control+"/control/scenarios", strings.Replace(create("process_drop", true), "normal", "commit_drop_once", 1), controlToken, false), 201)
	sender, err := delivery.NewHTTPSender(signing.Auth{Keys: testKeys{}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	dropClaim := delivery.ClaimedDelivery{DeliveryID: op, URLSnapshot: webhook + "/webhook", KeyRefSnapshot: "fulfillment-v1", WireBody: []byte(body("process_drop"))}
	firstDrop := sender.Send(ctx, dropClaim)
	if firstDrop.Outcome != delivery.Unconfirmed || firstDrop.HTTPStatus != nil {
		t.Fatal("no actual process response loss", firstDrop)
	}
	counts("process_drop", 1, 1)
	stop()
	stop = start()
	secondDrop := sender.Send(ctx, dropClaim)
	if secondDrop.Outcome != delivery.Acknowledged {
		t.Fatal("process restart rearmed drop", secondDrop)
	}
	counts("process_drop", 2, 1)
	out = inspect("process_drop")
	if out.DropRemaining != 0 {
		t.Fatal("consumed flag reset on process restart")
	}
	stop()
}
