//go:build integration

package receiver_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/receiver"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

// PUBLIC TEST-ONLY key/token. Never local runtime configuration.
const publicToken = "abababababababababababababababababababababababababababababababab"
const operation = "00000000-0000-4000-8000-000000000101"

type keys struct{}

func (keys) SigningKey(ref string) ([]byte, bool) {
	key, _ := hex.DecodeString(publicToken)
	return key, ref == "public-test-v1"
}
func fixture(t *testing.T, ctx context.Context) (*pgxpool.Pool, *store.DemoRepository, *receiver.Handler) {
	t.Helper()
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
	repo := store.NewDemoRepository(db.Pool, "fulfillment")
	return db.Pool, repo, receiver.New(repo, signing.Auth{Keys: keys{}}, publicToken, 3*time.Second)
}
func body(id string) []byte {
	return []byte(fmt.Sprintf(`{"event_id":"00000000-0000-4000-8000-000000000201","type":"order.confirmed","occurred_at":"2026-11-09T00:00:00Z","data":{"scenario_id":%q,"order_id":"order-1","quantity":2}}`, id))
}
func control(t *testing.T, ctx context.Context, url, method, path, raw string, want int) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, url+path, strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+publicToken)
	res, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != want {
		t.Fatalf("%s %s status %d want %d", method, path, res.StatusCode, want)
	}
}
func create(t *testing.T, ctx context.Context, url, id, mode string, dedupe bool, want int) {
	t.Helper()
	control(t, ctx, url, "POST", "/control/scenarios", fmt.Sprintf(`{"scenario_id":%q,"mode":%q,"dedupe_enabled":%t}`, id, mode, dedupe), want)
}
func inspect(t *testing.T, ctx context.Context, repo *store.DemoRepository, id string, requests, effects int64, drop int) {
	t.Helper()
	out, err := repo.Inspect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if out.RequestCount != requests || out.EffectCount != effects || out.DropRemaining != drop {
		t.Fatalf("scenario %s: %+v want requests/effects/drop %d/%d/%d", id, out, requests, effects, drop)
	}
}
func sender(t *testing.T, auth signing.Auth) *delivery.HTTPSender {
	t.Helper()
	s, err := delivery.NewHTTPSender(auth, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}
func claim(url, id string) delivery.ClaimedDelivery {
	return delivery.ClaimedDelivery{DeliveryID: operation, URLSnapshot: url + "/webhook", KeyRefSnapshot: "public-test-v1", WireBody: body(id)}
}

func TestCommitDropSocketAndDedupePostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, dedupe := range []bool{true, false} {
		t.Run(fmt.Sprintf("dedupe_%t", dedupe), func(t *testing.T) {
			pool, repo, h := fixture(t, ctx)
			controls := httptest.NewServer(h.Control())
			defer controls.Close()
			create(t, ctx, controls.URL, "drop_1", "commit_drop_once", dedupe, 201)
			var stamp atomic.Int64
			stamp.Store(time.Now().Unix())
			var mu sync.Mutex
			headers := []http.Header{}
			bodies := [][]byte{}
			web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(strings.NewReader(string(raw)))
				mu.Lock()
				headers = append(headers, r.Header.Clone())
				bodies = append(bodies, raw)
				mu.Unlock()
				h.Webhook().ServeHTTP(w, r)
			}))
			s := sender(t, signing.Auth{Keys: keys{}, Now: func() time.Time { return time.Unix(stamp.Load(), 0) }})
			c := claim(web.URL, "drop_1")
			first := s.Send(ctx, c)
			if first.Outcome != delivery.Unconfirmed || first.HTTPStatus != nil || first.ErrorCode != "network_error" {
				t.Fatal("first request did not lose actual response", first)
			}
			// DB state is observable immediately after the EOF: effect/request/flag must
			// already have committed, not merely be planned for an asynchronous write.
			inspect(t, ctx, repo, "drop_1", 1, 1, 0)
			var outcome string
			var added bool
			if err := pool.QueryRow(ctx, `SELECT planned_outcome,business_effect_added FROM demo_requests`).Scan(&outcome, &added); err != nil || outcome != "drop" || !added {
				t.Fatal("drop intent/effect not atomic", err)
			}
			// Restart the actual HTTP listener/handler/repository on the same DB.
			web.Close()
			repo = store.NewDemoRepository(pool, "fulfillment")
			h = receiver.New(repo, signing.Auth{Keys: keys{}}, publicToken, 3*time.Second)
			web = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(strings.NewReader(string(raw)))
				mu.Lock()
				headers = append(headers, r.Header.Clone())
				bodies = append(bodies, raw)
				mu.Unlock()
				h.Webhook().ServeHTTP(w, r)
			}))
			defer web.Close()
			// Repeated creation cannot rearm the consumed flag.
			create(t, ctx, controls.URL, "drop_1", "commit_drop_once", dedupe, 200)
			inspect(t, ctx, repo, "drop_1", 1, 1, 0)
			stamp.Add(1)
			c.URLSnapshot = web.URL + "/webhook"
			second := s.Send(ctx, c)
			if second.Outcome != delivery.Acknowledged || second.HTTPStatus == nil || *second.HTTPStatus != 204 {
				t.Fatal("retry failed", second)
			}
			effects := int64(1)
			if !dedupe {
				effects = 2
			}
			inspect(t, ctx, repo, "drop_1", 2, effects, 0)
			mu.Lock()
			if len(headers) != 2 || headers[0].Get("Webhook-Timestamp") == headers[1].Get("Webhook-Timestamp") || headers[0].Get("Webhook-Signature") == headers[1].Get("Webhook-Signature") || headers[0].Get("Webhook-Id") != headers[1].Get("Webhook-Id") || string(bodies[0]) != string(bodies[1]) {
				t.Error("retry identity/body or fresh timestamp wrong")
			}
			mu.Unlock()
			// Dedupe conflicts do not add effects or change the consumed flag.
			if dedupe {
				c.WireBody = append(c.WireBody, ' ')
				conflict := s.Send(ctx, c)
				if conflict.HTTPStatus == nil || *conflict.HTTPStatus != 409 {
					t.Fatal("changed body accepted")
				}
				inspect(t, ctx, repo, "drop_1", 3, 1, 0)
			}
			control(t, ctx, controls.URL, "PATCH", "/control/scenarios/drop_1", `{"mode":"normal"}`, 400)
		})
	}
}
func TestOutageControlsAndRollbackPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, repo, h := fixture(t, ctx)
	controls := httptest.NewServer(h.Control())
	defer controls.Close()
	web := httptest.NewServer(h.Webhook())
	defer web.Close()
	s := sender(t, signing.Auth{Keys: keys{}})
	for _, status := range []int{503, 429} {
		id := fmt.Sprintf("outage_%d", status)
		mode := fmt.Sprintf("http_%d", status)
		create(t, ctx, controls.URL, id, mode, true, 201)
		result := s.Send(ctx, claim(web.URL, id))
		if result.Outcome != delivery.HTTPFailure || result.HTTPStatus == nil || *result.HTTPStatus != status || len(result.RetryAfter) != 1 || result.RetryAfter[0] != "2" {
			t.Fatal("invalid outage response", result)
		}
		decision, err := (delivery.Policy{Jitter: func(lo, hi time.Duration) time.Duration { return lo }}).Decide(result, 1, time.Now())
		if err != nil || decision.RetryDelay != 2*time.Second {
			t.Fatal("Retry-After not honored", decision, err)
		}
		inspect(t, ctx, repo, id, 1, 0, 0)
		var rejected int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM demo_requests WHERE scenario_id=$1 AND planned_outcome=$2 AND NOT business_effect_added`, id, fmt.Sprintf("reject_%d", status)).Scan(&rejected); err != nil || rejected != 1 {
			t.Fatal("outage audit mismatch", err)
		}
		var ledger int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM demo_dedup WHERE scenario_id=$1`, id).Scan(&ledger); err != nil || ledger != 0 {
			t.Fatal("outage wrote ledger")
		}
		for _, raw := range []string{`{"mode":"http_429"}`, `{"mode":"commit_drop_once"}`, `{"mode":"normal","dedupe_enabled":false}`, `{"Mode":"normal"}`, `{"mode":"normal","mode":"normal"}`, `{}`, `null`} {
			control(t, ctx, controls.URL, "PATCH", "/control/scenarios/"+id, raw, 400)
		}
		control(t, ctx, controls.URL, "PATCH", "/control/scenarios/"+id, `{"mode":"normal"}`, 200)
		control(t, ctx, controls.URL, "PATCH", "/control/scenarios/"+id, `{"mode":"normal"}`, 200)
		result = s.Send(ctx, claim(web.URL, id))
		if result.Outcome != delivery.Acknowledged {
			t.Fatal("recovery failed", result)
		}
		inspect(t, ctx, repo, id, 2, 1, 0)
		out, _ := repo.Inspect(ctx, id)
		if out.Mode != "normal" || !out.DedupeEnabled {
			t.Fatal("recovery changed immutable dedupe")
		}
	}
	control(t, ctx, controls.URL, "PATCH", "/control/scenarios/missing", `{"mode":"normal"}`, 404)
	// A late request failure and a deferred COMMIT failure must restore the drop
	// flag as well as ledger/effect rows; the first subsequent success still drops.
	for _, kind := range []string{"request", "commit"} {
		id := "rollback_" + kind
		create(t, ctx, controls.URL, id, "commit_drop_once", true, 201)
		if kind == "request" {
			if _, err := pool.Exec(ctx, `ALTER TABLE demo_requests ADD CONSTRAINT reject_fault_request CHECK (scenario_id <> 'rollback_request')`); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_fault_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test commit rejection'; END $$; CREATE CONSTRAINT TRIGGER reject_fault_commit AFTER INSERT ON demo_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.scenario_id='rollback_commit') EXECUTE FUNCTION reject_fault_commit()`); err != nil {
				t.Fatal(err)
			}
		}
		result := s.Send(ctx, claim(web.URL, id))
		if result.HTTPStatus == nil || *result.HTTPStatus != 503 {
			t.Fatal("DB failure acknowledged/dropped", result)
		}
		inspect(t, ctx, repo, id, 0, 0, 1)
		var ledger int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM demo_dedup WHERE scenario_id=$1`, id).Scan(&ledger); err != nil || ledger != 0 {
			t.Fatal("orphan ledger")
		}
		if kind == "request" {
			if _, err := pool.Exec(ctx, `ALTER TABLE demo_requests DROP CONSTRAINT reject_fault_request`); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := pool.Exec(ctx, `DROP TRIGGER reject_fault_commit ON demo_requests; DROP FUNCTION reject_fault_commit()`); err != nil {
				t.Fatal(err)
			}
		}
		result = s.Send(ctx, claim(web.URL, id))
		if result.Outcome != delivery.Unconfirmed || result.HTTPStatus != nil {
			t.Fatal("rollback consumed drop", result)
		}
		inspect(t, ctx, repo, id, 1, 1, 0)
	}
	// Signature/body rejection must leave an armed scenario untouched.
	create(t, ctx, controls.URL, "armed", "commit_drop_once", true, 201)
	req, _ := http.NewRequestWithContext(ctx, "POST", web.URL+"/webhook", strings.NewReader(string(body("armed"))))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 401 {
		t.Fatal("unsigned fault accepted")
	}
	inspect(t, ctx, repo, "armed", 0, 0, 1)
	invalid := claim(web.URL, "armed")
	invalid.WireBody = []byte(`{}`)
	result := s.Send(ctx, invalid)
	if result.HTTPStatus == nil || *result.HTTPStatus != 400 {
		t.Fatal("signed malformed body accepted", result)
	}
	inspect(t, ctx, repo, "armed", 0, 0, 1)
}
func TestConcurrentDropConsumedOncePostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, repo, h := fixture(t, ctx)
	if _, err := repo.CreateScenario(ctx, "race", "commit_drop_once", true); err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(h.Webhook())
	defer web.Close()
	s := sender(t, signing.Auth{Keys: keys{}})
	results := make(chan delivery.SendResult, 8)
	gate := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() { <-gate; results <- s.Send(ctx, claim(web.URL, "race")) }()
	}
	close(gate)
	drops := 0
	for i := 0; i < 8; i++ {
		result := <-results
		if result.Outcome == delivery.Unconfirmed && result.HTTPStatus == nil {
			drops++
		} else if result.Outcome != delivery.Acknowledged {
			t.Fatal(result)
		}
	}
	if drops != 1 {
		t.Fatal("drop flag not consumed exactly once", drops)
	}
	inspect(t, ctx, repo, "race", 8, 1, 0)
}
