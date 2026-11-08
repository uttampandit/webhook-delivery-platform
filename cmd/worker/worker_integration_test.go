//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/api"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/receiver"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

// PUBLIC TEST-ONLY fixtures; never runtime configuration.
const testSecret = "abababababababababababababababababababababababababababababababab"

type fixtureKeys struct{}

func (fixtureKeys) SigningKey(ref string) ([]byte, bool) {
	key, _ := hex.DecodeString(testSecret)
	return key, ref == config.FulfillmentKeyRef || ref == config.NotificationKeyRef
}
func TestWorkerHelper(t *testing.T) {
	if os.Getenv("WEBHOOK_WORKER_TEST_PROCESS") != "1" {
		return
	}
	if run() != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
func workerDB(t *testing.T, ctx context.Context) *testutil.Database {
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
	return db
}
func fixtureRegistry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fulfillment, notification string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO tenants(id,name) VALUES($1,'worker fixture')`, config.TenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO destinations(id,tenant_id,code,url,key_ref) VALUES($1,$2,'fulfillment',$3,$4),($5,$2,'notification',$6,$7)`, config.FulfillmentDestination, config.TenantID, fulfillment, config.FulfillmentKeyRef, config.NotificationDestination, notification, config.NotificationKeyRef); err != nil {
		t.Fatal(err)
	}
}
func startWorker(t *testing.T, ctx context.Context, db *testutil.Database, concurrency ...string) func() {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	target, err := url.Parse(db.AdminConnectionString())
	if err != nil {
		t.Fatal("invalid administrator URL")
	}
	target.Path = "/" + db.Name()
	target.RawPath = ""
	query := target.Query()
	query.Del("dbname")
	target.RawQuery = query.Encode()
	overrides := map[string]string{"WEBHOOK_WORKER_TEST_PROCESS": "1", "DATABASE_URL": target.String(), "RUN_MODE": "host", "FULFILLMENT_SIGNING_KEY_V1": testSecret, "NOTIFICATION_SIGNING_KEY_V1": testSecret, "WORKER_CONCURRENCY": "4", "WORKER_POLL_INTERVAL": "20ms"}
	if len(concurrency) > 0 {
		overrides["WORKER_CONCURRENCY"] = concurrency[0]
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestWorkerHelper$")
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, ok := overrides[name]; !ok {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	for k, v := range overrides {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	// Fixed redacted production diagnostics only; never print environment/DSN.
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err = cmd.Start(); err != nil {
		t.Fatal("start worker", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			start := time.Now()
			cmd.Process.Signal(syscall.SIGTERM)
			select {
			case err := <-done:
				if err != nil {
					t.Error("worker did not stop cleanly")
				}
				if time.Since(start) >= 10*time.Second {
					t.Error("SIGTERM exceeded 10s")
				}
			case <-time.After(10 * time.Second):
				cmd.Process.Kill()
				<-done
				t.Error("SIGTERM timed out")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}
func awaitWorker(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("worker state deadline")
		}
	}
}
func apiFixtureSettings(t *testing.T) config.Settings {
	t.Helper()
	values := map[string]string{"DATABASE_URL": "postgres://test:public@localhost/test", "RUN_MODE": "host", "API_TOKEN": testSecret}
	s, err := config.LoadFrom(config.ServiceAPI, func(k string) (string, bool) { v, ok := values[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func ingestWorkerEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, destinations []string) string {
	t.Helper()
	handler := httptest.NewServer(api.NewHandler(apiFixtureSettings(t), store.NewAPIReadRepository(pool)))
	defer handler.Close()
	quoted := []string{}
	for _, id := range destinations {
		quoted = append(quoted, fmt.Sprintf("%q", id))
	}
	body := fmt.Sprintf(`{"type":"order.confirmed","payload":{"scenario_id":"worker_1","order_id":"order-1","quantity":2,"extra":1e1000},"destination_ids":[%s]}`, strings.Join(quoted, ","))
	for i, want := range []int{202, 200} {
		request, _ := http.NewRequestWithContext(ctx, "POST", handler.URL+"/v1/events", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+testSecret)
		request.Header.Set("Idempotency-Key", "worker-fixture-idempotency")
		request.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: time.Second}).Do(request)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != want {
			t.Fatal("ingest", i, response.StatusCode)
		}
	}
	var id string
	var events, deliveries int
	if err := pool.QueryRow(ctx, `SELECT min(id::text),count(*) FROM events`).Scan(&id, &events); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deliveries`).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if events != 1 || deliveries != len(destinations) {
		t.Fatal("duplicate ingest added rows")
	}
	var wire []byte
	if err := pool.QueryRow(ctx, `SELECT wire_body FROM events WHERE id=$1`, id).Scan(&wire); err != nil || !bytes.Contains(wire, []byte(`"extra":1e1000`)) {
		t.Fatal("numeric lexeme changed in persisted wire body", err)
	}
	return id
}
func TestWorkerSignedVerticalSliceAndIndependentDestination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	db := workerDB(t, ctx)
	repo := store.NewDemoRepository(db.Pool, "fulfillment")
	if _, err := repo.CreateScenario(ctx, "worker_1", "normal", true); err != nil {
		t.Fatal(err)
	}
	realReceiver := receiver.New(repo, signing.Auth{Keys: fixtureKeys{}}, testSecret, time.Second).Webhook()
	var lockProbes atomic.Int32
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// During actual HTTP, attempt already exists and delivery lock is free.
		probe, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		tx, err := db.Pool.Begin(probe)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		defer tx.Rollback(probe)
		var count int
		if _, err = tx.Exec(probe, `SELECT id FROM deliveries WHERE id=$1 FOR UPDATE NOWAIT`, r.Header.Get("Webhook-Id")); err != nil {
			t.Error("HTTP under delivery lock", err)
		} else if err = tx.QueryRow(probe, `SELECT count(*) FROM attempts WHERE delivery_id=$1 AND outcome='running'`, r.Header.Get("Webhook-Id")).Scan(&count); err != nil || count != 1 {
			t.Error("HTTP before persisted running attempt", err)
		} else {
			lockProbes.Add(1)
		}
		if err = tx.Commit(probe); err != nil {
			t.Error(err)
		}
		realReceiver.ServeHTTP(w, r)
	}))
	defer healthy.Close()
	var rejected atomic.Int32
	outage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if _, err := (signing.Auth{Keys: fixtureKeys{}}).Verify(r.Header, body); err != nil {
			t.Error("outage fixture signature", err)
		}
		rejected.Add(1)
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(503)
	}))
	defer outage.Close()
	fixtureRegistry(t, ctx, db.Pool, healthy.URL+"/webhook", outage.URL+"/webhook")
	event := ingestWorkerEvent(t, ctx, db.Pool, []string{config.FulfillmentDestination, config.NotificationDestination})
	stop := startWorker(t, ctx, db)
	awaitWorker(t, ctx, func() bool {
		var count int
		err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE event_id=$1 AND destination_id=$2 AND status='succeeded'`, event, config.FulfillmentDestination).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			var observed string
			if err := db.Pool.QueryRow(ctx, `SELECT string_agg(d.status||':'||COALESCE(a.outcome,'none')||':'||COALESCE(a.http_status::text,'none')||':'||COALESCE(a.error_code,'none'),',') FROM deliveries d LEFT JOIN attempts a ON a.delivery_id=d.id WHERE d.destination_id=$1`, config.FulfillmentDestination).Scan(&observed); err == nil && strings.Contains(observed, "dead") {
				t.Fatal("healthy destination became terminal:", observed)
			}
		}
		return count == 1
	})
	awaitWorker(t, ctx, func() bool {
		var count int
		err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM attempts a JOIN deliveries d ON d.id=a.delivery_id WHERE d.destination_id=$1 AND a.outcome='http_failure'`, config.NotificationDestination).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		return count >= 1
	})
	out, err := repo.Inspect(ctx, "worker_1")
	if err != nil || out.EffectCount != 1 || out.RequestCount != 1 || lockProbes.Load() != 1 {
		t.Fatal("healthy signed flow", out, err)
	}
	var delay int64
	var finished, next time.Time
	if err = db.Pool.QueryRow(ctx, `SELECT a.retry_delay_ms,a.finished_at,d.next_attempt_at FROM attempts a JOIN deliveries d ON d.id=a.delivery_id WHERE d.destination_id=$1 AND a.attempt_no=1`, config.NotificationDestination).Scan(&delay, &finished, &next); err != nil {
		t.Fatal(err)
	}
	if delay != 30000 || next.Sub(finished) != 30*time.Second || rejected.Load() < 1 {
		t.Fatal("503 did not independently schedule retry", delay)
	}
	stop()
}
func TestWorkerSIGTERMCancelsAndPersistsAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	db := workerDB(t, ctx)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		once.Do(func() { close(started) })
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	fixtureRegistry(t, ctx, db.Pool, server.URL, server.URL)
	ingestWorkerEvent(t, ctx, db.Pool, []string{config.FulfillmentDestination})
	stop := startWorker(t, ctx, db)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not send")
	}
	stop()
	var outcome, code, status string
	var finished bool
	if err := db.Pool.QueryRow(ctx, `SELECT a.outcome,a.error_code,a.finished_at IS NOT NULL,d.status FROM attempts a JOIN deliveries d ON d.id=a.delivery_id WHERE a.attempt_no=1`).Scan(&outcome, &code, &finished, &status); err != nil {
		t.Fatal(err)
	}
	if outcome != "unconfirmed" || code != "canceled" || !finished || status != "pending" {
		t.Fatal("canceled attempt not safely persisted", outcome, code, status)
	}
}

func TestWorkerReapsWithAllSlotsBusy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	db := workerDB(t, ctx)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		once.Do(func() { close(started) })
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	fixtureRegistry(t, ctx, db.Pool, server.URL, server.URL)
	ingestWorkerEvent(t, ctx, db.Pool, []string{config.FulfillmentDestination, config.NotificationDestination})
	if _, e := db.Pool.Exec(ctx, `UPDATE deliveries SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE destination_id=$1`, config.NotificationDestination); e != nil {
		t.Fatal(e)
	}
	claims, e := store.NewClaimRepository(db.Pool, 30*time.Second, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	abandoned, e := claims.ClaimDue(ctx, 1)
	if e != nil || len(abandoned) != 1 {
		t.Fatal(e)
	}
	// This repository fixture models an abandoned committed claim; actual SIGKILL
	// synchronization remains Task 19. No attempt or counter is erased.
	if _, e = db.Pool.Exec(ctx, `UPDATE deliveries SET lease_until=clock_timestamp() WHERE id=$1`, abandoned[0].DeliveryID); e != nil {
		t.Fatal(e)
	}
	if _, e = db.Pool.Exec(ctx, `UPDATE deliveries SET next_attempt_at=clock_timestamp() WHERE destination_id=$1`, config.NotificationDestination); e != nil {
		t.Fatal(e)
	}
	stop := startWorker(t, ctx, db, "1")
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("sender not busy")
	}
	awaitWorker(t, ctx, func() bool {
		var recovered bool
		if e := db.Pool.QueryRow(ctx, `SELECT a.outcome='unconfirmed' AND a.error_code='lease_expired' AND EXISTS(SELECT 1 FROM deliveries WHERE destination_id=$2 AND status='in_flight') FROM attempts a WHERE a.id=$1`, abandoned[0].AttemptID, config.NotificationDestination).Scan(&recovered); e != nil {
			t.Fatal(e)
		}
		return recovered
	})
	stop()
	finalizer, _ := store.NewFinalizationRepository(db.Pool, time.Second, delivery.Policy{})
	code := 204
	if _, e = finalizer.Finalize(ctx, abandoned[0], delivery.SendResult{Outcome: delivery.Acknowledged, HTTPStatus: &code}); e != store.ErrStaleClaim {
		t.Fatal(e)
	}
}
