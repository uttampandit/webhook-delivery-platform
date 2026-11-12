//go:build crash && (darwin || linux)

package crash_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/receiver"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

// Public test-only fixture key. It is never used by local runtime configuration.
const crashSigningSecret = "abababababababababababababababababababababababababababababababab"

type crashKeys struct{}

func (crashKeys) SigningKey(ref string) ([]byte, bool) {
	key, _ := hex.DecodeString(crashSigningSecret)
	return key, ref == config.FulfillmentKeyRef || ref == config.NotificationKeyRef
}

type workerBinaries struct {
	testhooks string
	release   string
}

var (
	binariesMu  sync.Mutex
	binaries    workerBinaries
	binariesDir string
)

func TestMain(m *testing.M) {
	code := m.Run()
	if binariesDir != "" {
		_ = os.RemoveAll(binariesDir)
	}
	os.Exit(code)
}

func workerExecutables(t *testing.T) workerBinaries {
	t.Helper()
	binariesMu.Lock()
	defer binariesMu.Unlock()
	if binaries.testhooks != "" {
		return binaries
	}
	var err error
	binariesDir, err = os.MkdirTemp("", "webhook-crash-worker-")
	if err != nil {
		t.Fatal("create worker build directory")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal("resolve repository root")
	}
	binaries = workerBinaries{
		testhooks: filepath.Join(binariesDir, "worker-testhooks"),
		release:   filepath.Join(binariesDir, "worker-release"),
	}
	for _, build := range []struct {
		path string
		tags string
	}{{binaries.testhooks, "testhooks"}, {binaries.release, ""}} {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		args := []string{"build", "-o", build.path}
		if build.tags != "" {
			args = append(args, "-tags", build.tags)
		}
		args = append(args, "./cmd/worker")
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = root
		command.Env = os.Environ()
		if output, buildErr := command.CombinedOutput(); buildErr != nil {
			cancel()
			_ = output // Do not expose an environment or credential-bearing build log.
			t.Fatalf("build worker subprocess (tags=%q): %v", build.tags, buildErr)
		}
		cancel()
	}
	return binaries
}

type crashFixture struct {
	database    *testutil.Database
	databaseURL string
	deliveryID  string
	scenarioID  string
	receiver    *store.DemoRepository
}

func newCrashFixture(t *testing.T, ctx context.Context, scenarioID string) crashFixture {
	t.Helper()
	database, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal("resolve repository root")
	}
	if err = store.Apply(ctx, database.Pool, filepath.Join(root, "migrations")); err != nil {
		t.Fatal("apply migrations:", err)
	}

	receiverRepo := store.NewDemoRepository(database.Pool, "fulfillment")
	if _, err = receiverRepo.CreateScenario(ctx, scenarioID, "normal", true); err != nil {
		t.Fatal("create receiver scenario:", err)
	}
	server := httptest.NewServer(receiver.New(receiverRepo, signing.Auth{Keys: crashKeys{}}, "", 2*time.Second).Webhook())
	t.Cleanup(server.Close)
	if _, err = database.Pool.Exec(ctx, `INSERT INTO tenants(id,name) VALUES($1,'crash test tenant')`, config.TenantID); err != nil {
		t.Fatal("seed tenant:", err)
	}
	if _, err = database.Pool.Exec(ctx, `
		INSERT INTO destinations(id,tenant_id,code,url,key_ref)
		VALUES($1,$3,'fulfillment',$4,$5),($2,$3,'notification','http://127.0.0.1:1/webhook',$6)`,
		config.FulfillmentDestination, config.NotificationDestination, config.TenantID,
		server.URL+"/webhook", config.FulfillmentKeyRef, config.NotificationKeyRef); err != nil {
		t.Fatal("seed tenant and destination snapshots:", err)
	}
	raw := []byte(fmt.Sprintf(`{"type":"order.confirmed","payload":{"scenario_id":%q,"order_id":"order-1","quantity":1},"destination_ids":[%q]}`,
		scenarioID, config.FulfillmentDestination))
	request, err := store.ValidateEventRequest(raw)
	if err != nil {
		t.Fatal("validate event fixture:", err)
	}
	accepted, err := store.NewEventRepository(database.Pool).AcceptEvent(ctx, config.TenantID, "crash-"+scenarioID, request)
	if err != nil || len(accepted.DeliveryIDs) != 1 {
		t.Fatal("accept event fixture:", err)
	}
	adminURL, err := url.Parse(database.AdminConnectionString())
	if err != nil {
		t.Fatal("parse test administrator URL")
	}
	adminURL.Path = "/" + database.Name()
	adminURL.RawPath = ""
	query := adminURL.Query()
	query.Del("dbname")
	adminURL.RawQuery = query.Encode()
	return crashFixture{database: database, databaseURL: adminURL.String(), deliveryID: accepted.DeliveryIDs[0], scenarioID: scenarioID, receiver: receiverRepo}
}

func workerEnvironment(fixture crashFixture, concurrency string) map[string]string {
	return map[string]string{
		"DATABASE_URL":                fixture.databaseURL,
		"RUN_MODE":                    "host",
		"FULFILLMENT_SIGNING_KEY_V1":  crashSigningSecret,
		"NOTIFICATION_SIGNING_KEY_V1": crashSigningSecret,
		"WORKER_CONCURRENCY":          concurrency,
		"WORKER_POLL_INTERVAL":        "20ms",
		"LEASE_DURATION":              "3s",
		"LEASE_MARGIN":                "500ms",
		"HTTP_TIMEOUT":                "300ms",
		"DB_STATEMENT_TIMEOUT":        "1s",
	}
}

func startWorker(t *testing.T, binary string, fixture crashFixture, concurrency string, hookStage string) *testutil.ChildProcess {
	t.Helper()
	environment := workerEnvironment(fixture, concurrency)
	if hookStage != "" {
		environment["WEBHOOK_TEST_HOOK_ENABLE"] = "1"
		environment["WEBHOOK_TEST_HOOK_STAGE"] = hookStage
		environment["WEBHOOK_TEST_HOOK_OPERATION_ID"] = fixture.deliveryID
		environment["WEBHOOK_TEST_HOOK_ATTEMPT_NO"] = "1"
	}
	child, err := testutil.StartChildProcess(binary, environment)
	if err != nil {
		t.Fatal("start worker subprocess:", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = child.Close(ctx)
	})
	return child
}

func crashContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type persistedAttempt struct {
	number  int
	outcome string
	error   string
}

type persistedState struct {
	status   string
	cycle    int
	lifetime int
	attempts []persistedAttempt
}

func readState(ctx context.Context, pool *pgxpool.Pool, deliveryID string) (persistedState, error) {
	state := persistedState{attempts: []persistedAttempt{}}
	if err := pool.QueryRow(ctx, `SELECT status,cycle_attempt_count,lifetime_attempt_count FROM deliveries WHERE id=$1`, deliveryID).Scan(&state.status, &state.cycle, &state.lifetime); err != nil {
		return state, err
	}
	rows, err := pool.Query(ctx, `SELECT attempt_no,outcome,COALESCE(error_code,'') FROM attempts WHERE delivery_id=$1 ORDER BY attempt_no`, deliveryID)
	if err != nil {
		return state, err
	}
	defer rows.Close()
	for rows.Next() {
		var attempt persistedAttempt
		if err := rows.Scan(&attempt.number, &attempt.outcome, &attempt.error); err != nil {
			return state, err
		}
		state.attempts = append(state.attempts, attempt)
	}
	return state, rows.Err()
}

func awaitState(t *testing.T, ctx context.Context, fixture crashFixture, predicate func(persistedState) bool) persistedState {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var last persistedState
	for {
		state, err := readState(ctx, fixture.database.Pool, fixture.deliveryID)
		if err == nil {
			last = state
			if predicate(state) {
				return state
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("delivery state deadline: status=%s cycle=%d lifetime=%d attempts=%+v err=%v", last.status, last.cycle, last.lifetime, last.attempts, ctx.Err())
		}
	}
}

func readCheckpoint(t *testing.T, ctx context.Context, child *testutil.ChildProcess, stage, operation string, attempt int) {
	t.Helper()
	line, err := child.ReadLine(ctx)
	if err != nil {
		t.Fatal("worker did not reach the requested process checkpoint:", err)
	}
	var got struct {
		Stage       string `json:"stage"`
		OperationID string `json:"operation_id"`
		AttemptNo   int    `json:"attempt_no"`
	}
	if err := json.Unmarshal([]byte(line), &got); err != nil || got.Stage != stage || got.OperationID != operation || got.AttemptNo != attempt {
		t.Fatalf("unexpected machine checkpoint %q: %+v (%v)", line, got, err)
	}
}

func killWorker(t *testing.T, ctx context.Context, child *testutil.ChildProcess) {
	t.Helper()
	if err := child.Kill(); err != nil {
		t.Fatal("send SIGKILL to worker:", err)
	}
	if err := child.Wait(ctx); err == nil {
		t.Fatal("SIGKILL worker exited without a signal status")
	}
	if !child.KilledBySIGKILL() {
		t.Fatal("worker was not terminated by SIGKILL")
	}
}

func stopWorker(t *testing.T, ctx context.Context, child *testutil.ChildProcess) {
	t.Helper()
	if err := child.Signal(syscall.SIGTERM); err != nil {
		t.Fatal("send SIGTERM to worker:", err)
	}
	if err := child.Wait(ctx); err != nil {
		t.Fatal("worker did not stop cleanly:", err)
	}
}

func assertRecoveredAttempts(t *testing.T, state persistedState) {
	t.Helper()
	if state.status != "succeeded" || state.lifetime != 2 || len(state.attempts) != 2 ||
		state.attempts[0] != (persistedAttempt{number: 1, outcome: "unconfirmed", error: "lease_expired"}) ||
		state.attempts[1] != (persistedAttempt{number: 2, outcome: "acknowledged"}) {
		t.Fatalf("recovered attempts did not preserve the consumed lifetime number: %+v", state)
	}
}

func assertEffects(t *testing.T, ctx context.Context, fixture crashFixture, minRequests, wantEffects int64) {
	t.Helper()
	result, err := fixture.receiver.Inspect(ctx, fixture.scenarioID)
	if err != nil || result.RequestCount < minRequests || result.EffectCount != wantEffects {
		t.Fatalf("receiver counts requests=%d effects=%d want requests>=%d effects=%d: %v", result.RequestCount, result.EffectCount, minRequests, wantEffects, err)
	}
	if minRequests == 0 && result.RequestCount == 0 {
		if len(result.OperationCounts) != 0 {
			t.Fatalf("receiver recorded an operation before any HTTP request: %+v", result.OperationCounts)
		}
		return
	}
	if len(result.OperationCounts) != 1 || result.OperationCounts[0].OperationID != fixture.deliveryID ||
		result.OperationCounts[0].RequestCount < minRequests || result.OperationCounts[0].EffectCount != wantEffects {
		t.Fatalf("receiver did not retain stable delivery operation %s: %+v", fixture.deliveryID, result)
	}
}

func TestCrashAfterClaimCommitRecoversConsumedAttempt(t *testing.T) {
	ctx := crashContext(t)
	bins := workerExecutables(t)
	fixture := newCrashFixture(t, ctx, "crash_after_claim")
	paused := startWorker(t, bins.testhooks, fixture, "1", "after_claim_commit")
	readCheckpoint(t, ctx, paused, "after_claim_commit", fixture.deliveryID, 1)
	state, err := readState(ctx, fixture.database.Pool, fixture.deliveryID)
	if err != nil || state.status != "in_flight" || state.lifetime != 1 || len(state.attempts) != 1 || state.attempts[0].outcome != "running" {
		t.Fatalf("claim and running attempt were not committed before the checkpoint: %+v %v", state, err)
	}
	assertEffects(t, ctx, fixture, 0, 0)
	killWorker(t, ctx, paused)
	state, err = readState(ctx, fixture.database.Pool, fixture.deliveryID)
	if err != nil || state.status != "in_flight" || state.attempts[0].outcome != "running" {
		t.Fatalf("SIGKILL removed or finalized the committed attempt: %+v %v", state, err)
	}

	restarted := startWorker(t, bins.release, fixture, "1", "")
	state = awaitState(t, ctx, fixture, func(state persistedState) bool { return state.status == "succeeded" })
	assertRecoveredAttempts(t, state)
	assertEffects(t, ctx, fixture, 1, 1)
	stopWorker(t, ctx, restarted)
}

func TestCrashAfterReceiverEffectRepeatsRequestButKeepsOneEffect(t *testing.T) {
	ctx := crashContext(t)
	bins := workerExecutables(t)
	fixture := newCrashFixture(t, ctx, "crash_after_send")
	paused := startWorker(t, bins.testhooks, fixture, "1", "after_send_before_finalize")
	readCheckpoint(t, ctx, paused, "after_send_before_finalize", fixture.deliveryID, 1)
	state, err := readState(ctx, fixture.database.Pool, fixture.deliveryID)
	if err != nil || state.status != "in_flight" || state.attempts[0].outcome != "running" {
		t.Fatalf("worker reached response checkpoint without a committed running attempt: %+v %v", state, err)
	}
	assertEffects(t, ctx, fixture, 1, 1)
	killWorker(t, ctx, paused)
	state, err = readState(ctx, fixture.database.Pool, fixture.deliveryID)
	if err != nil || state.status != "in_flight" || state.attempts[0].outcome != "running" {
		t.Fatalf("SIGKILL changed the unfinalized attempt: %+v %v", state, err)
	}

	restarted := startWorker(t, bins.release, fixture, "1", "")
	state = awaitState(t, ctx, fixture, func(state persistedState) bool { return state.status == "succeeded" })
	assertRecoveredAttempts(t, state)
	assertEffects(t, ctx, fixture, 2, 1)
	stopWorker(t, ctx, restarted)
}

func TestPausedStaleWorkerCannotFinalizeAfterReplacementClaim(t *testing.T) {
	ctx := crashContext(t)
	bins := workerExecutables(t)
	fixture := newCrashFixture(t, ctx, "crash_stale_sender")
	paused := startWorker(t, bins.testhooks, fixture, "2", "after_send_before_finalize")
	readCheckpoint(t, ctx, paused, "after_send_before_finalize", fixture.deliveryID, 1)
	assertEffects(t, ctx, fixture, 1, 1)

	// The worker's independent reaper expires the paused lease while its sender
	// goroutine remains blocked; its second slot can claim and finish the retry.
	state := awaitState(t, ctx, fixture, func(state persistedState) bool { return state.status == "succeeded" && state.lifetime == 2 })
	assertRecoveredAttempts(t, state)
	assertEffects(t, ctx, fixture, 2, 1)
	if err := paused.Continue(); err != nil {
		t.Fatal("continue the old worker after replacement succeeded:", err)
	}
	if err := paused.WaitForStderr(ctx, "stale finalization rejected"); err != nil {
		t.Fatal("old worker did not reach the fenced stale-finalization path:", err)
	}
	state, err := readState(ctx, fixture.database.Pool, fixture.deliveryID)
	if err != nil {
		t.Fatal("read state after stale finalization:", err)
	}
	assertRecoveredAttempts(t, state)
	assertEffects(t, ctx, fixture, 2, 1)
	stopWorker(t, ctx, paused)
}

func TestReleaseWorkerCannotActivateHookEnvironment(t *testing.T) {
	ctx := crashContext(t)
	bins := workerExecutables(t)
	fixture := newCrashFixture(t, ctx, "release_ignores_hook")
	worker := startWorker(t, bins.release, fixture, "1", "after_claim_commit")
	state := awaitState(t, ctx, fixture, func(state persistedState) bool { return state.status == "succeeded" })
	if state.lifetime != 1 || len(state.attempts) != 1 || state.attempts[0].outcome != "acknowledged" {
		t.Fatalf("release worker did not continue through the configured hook target: %+v", state)
	}
	assertEffects(t, ctx, fixture, 1, 1)
	stopWorker(t, ctx, worker)
}
