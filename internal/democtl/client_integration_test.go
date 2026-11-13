//go:build integration

package democtl

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/api"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/receiver"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

type testKeys struct{}

func (testKeys) SigningKey(string) ([]byte, bool) {
	b, _ := hex.DecodeString(testToken)
	return b, true
}
func TestCommandsActualAPIAndReceiversPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, e := testutil.NewDatabase(ctx, t)
	if e != nil {
		t.Fatal(e)
	}
	dir, e := filepath.Abs("../../migrations")
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Apply(ctx, db.Pool, dir); e != nil {
		t.Fatal(e)
	}
	s := settings(t)
	if e = store.Seed(ctx, db.Pool, s.Registry()); e != nil {
		t.Fatal(e)
	}
	controls := map[string]string{}
	for _, code := range []string{"fulfillment", "notification"} {
		h := receiver.New(store.NewDemoRepository(db.Pool, code), signing.Auth{Keys: testKeys{}}, testToken, time.Second)
		control := httptest.NewServer(h.Control())
		defer control.Close()
		controls[code] = control.URL
		webhook := httptest.NewServer(h.Webhook())
		defer webhook.Close()
		// Test-only endpoint fixture; democtl itself never opens a database.
		if _, e = db.Pool.Exec(ctx, `UPDATE destinations SET url=$1 WHERE code=$2`, webhook.URL+"/webhook", code); e != nil {
			t.Fatal(e)
		}
	}
	apiServer := httptest.NewServer(api.NewHandler(s, store.NewAPIReadRepository(db.Pool)))
	defer apiServer.Close()
	c := New(s, Endpoints{API: apiServer.URL, Controls: controls})
	c.PollInterval = time.Millisecond
	createArgs := []string{"scenario", "create", "--id", "repeatable", "--receiver", "both", "--mode", "http_503", "--dedupe", "on"}
	var first, second struct {
		ScenarioID string `json:"scenario_id"`
	}
	if e = json.Unmarshal([]byte(invoke(t, c, 0, createArgs...)), &first); e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal([]byte(invoke(t, c, 0, createArgs...)), &second); e != nil {
		t.Fatal(e)
	}
	if first.ScenarioID == second.ScenarioID {
		t.Fatal("reused old scenario")
	}
	invoke(t, c, 0, "scenario", "inspect", "--id", first.ScenarioID, "--receiver", "both")
	for _, code := range []string{"fulfillment", "notification"} {
		invoke(t, c, 0, "scenario", "recover", "--id", first.ScenarioID, "--receiver", code)
	}
	args := []string{"publish", "--scenario", first.ScenarioID, "--order", "order-1", "--destinations", "notification,fulfillment"}
	published := invoke(t, c, 0, args...)
	if duplicate := invoke(t, c, 0, args...); published != duplicate {
		t.Fatal("HTTP retry created different event")
	}
	var result struct {
		EventID     string   `json:"event_id"`
		DeliveryIDs []string `json:"delivery_ids"`
	}
	if e = json.Unmarshal([]byte(published), &result); e != nil {
		t.Fatal(e)
	}
	claims, e := store.NewClaimRepository(db.Pool, 30*time.Second, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	finalizer, e := store.NewFinalizationRepository(db.Pool, time.Second, delivery.Policy{})
	if e != nil {
		t.Fatal(e)
	}
	sender, e := delivery.NewHTTPSender(signing.Auth{Keys: testKeys{}}, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer sender.Close()
	claimed, e := claims.ClaimDue(ctx, 2)
	if e != nil || len(claimed) != 2 {
		t.Fatalf("claims=%d err=%v", len(claimed), e)
	}
	// One real receiver acknowledgment and one terminal outcome exercise replay.
	for i, claim := range claimed {
		sent := sender.Send(ctx, claim)
		if sent.Outcome != delivery.Acknowledged {
			t.Fatalf("send=%+v", sent)
		}
		if i == 0 {
			status := 400
			sent = delivery.SendResult{Outcome: delivery.HTTPFailure, HTTPStatus: &status}
		}
		if _, e = finalizer.Finalize(ctx, claim, sent); e != nil {
			t.Fatal(e)
		}
	}
	invoke(t, c, 1, "wait", "--event", result.EventID, "--state", "succeeded", "--timeout", "20ms")
	id := claimed[0].DeliveryID
	replay := invoke(t, c, 0, "replay", "--delivery", id, "--key", "explicit-replay-1")
	var replayResult struct {
		ReplayID     string `json:"replay_id"`
		Cycle        int    `json:"cycle_no"`
		Deduplicated bool   `json:"deduplicated"`
	}
	if e = json.Unmarshal([]byte(replay), &replayResult); e != nil || replayResult.Cycle != 2 {
		t.Fatal(e, replay)
	}
	originalID := replayResult.ReplayID
	if e = json.Unmarshal([]byte(invoke(t, c, 0, "replay", "--delivery", id, "--key", "explicit-replay-1")), &replayResult); e != nil || !replayResult.Deduplicated || replayResult.ReplayID != originalID {
		t.Fatal(e, replayResult)
	}
	claimed, e = claims.ClaimDue(ctx, 2)
	if e != nil || len(claimed) != 1 {
		t.Fatal(e, len(claimed))
	}
	if _, e = finalizer.Finalize(ctx, claimed[0], sender.Send(ctx, claimed[0])); e != nil {
		t.Fatal(e)
	}
	invoke(t, c, 0, "wait", "--event", result.EventID, "--state", "succeeded", "--timeout", "1s")
	for _, code := range []string{"fulfillment", "notification"} {
		summary, e := store.NewDemoRepository(db.Pool, code).Inspect(ctx, first.ScenarioID)
		if e != nil || summary.EffectCount != 1 {
			t.Fatalf("%s summary=%+v err=%v", code, summary, e)
		}
	}
}

func TestRunOutageObservationRecoversBeforeRetryBudgetExhaustion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Apply(ctx, db.Pool, filepath.Join(root, "migrations")); err != nil {
		t.Fatal(err)
	}
	s := settings(t)
	if err = store.Seed(ctx, db.Pool, s.Registry()); err != nil {
		t.Fatal(err)
	}
	controls := map[string]string{}
	for _, code := range []string{"fulfillment", "notification"} {
		h := receiver.New(store.NewDemoRepository(db.Pool, code), signing.Auth{Keys: testKeys{}}, testToken, time.Second)
		control := httptest.NewServer(h.Control())
		defer control.Close()
		controls[code] = control.URL
		webhook := httptest.NewServer(h.Webhook())
		defer webhook.Close()
		if _, err = db.Pool.Exec(ctx, `UPDATE destinations SET url=$1 WHERE code=$2`, webhook.URL+"/webhook", code); err != nil {
			t.Fatal(err)
		}
	}
	apiServer := httptest.NewServer(api.NewHandler(s, store.NewAPIReadRepository(db.Pool)))
	defer apiServer.Close()
	c := New(s, Endpoints{API: apiServer.URL, Controls: controls})
	c.PollInterval = time.Millisecond

	scenarioID, err := c.createScenario(ctx, "retry_before_exhaustion", "both", "http_503", "on")
	if err != nil {
		t.Fatal("create outage scenario:", err)
	}
	if err = c.recoverScenario(ctx, scenarioID, "notification"); err != nil {
		t.Fatal("recover notification before publishing:", err)
	}
	eventID, _, err := c.publishScenario(ctx, scenarioID, "retry-before-exhaustion-order", []string{"fulfillment", "notification"})
	if err != nil {
		t.Fatal("publish event:", err)
	}

	claims, err := store.NewClaimRepository(db.Pool, 30*time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	finalizer, err := store.NewFinalizationRepository(db.Pool, time.Second, delivery.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	sender, err := delivery.NewHTTPSender(signing.Auth{Keys: testKeys{}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	claimed, err := claims.ClaimDue(ctx, 2)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("initial claims=%d err=%v, want two fan-out deliveries", len(claimed), err)
	}
	var fulfillmentDeliveryID string
	for _, claim := range claimed {
		sent := sender.Send(ctx, claim)
		finalized, finalizeErr := finalizer.Finalize(ctx, claim, sent)
		if finalizeErr != nil {
			t.Fatal("finalize first fan-out attempt:", finalizeErr)
		}
		if sent.Outcome == delivery.HTTPFailure {
			fulfillmentDeliveryID = claim.DeliveryID
		}
		if sent.Outcome == delivery.HTTPFailure && (sent.HTTPStatus == nil || *sent.HTTPStatus != 503 || finalized.Status != "pending") {
			t.Fatalf("fulfillment did not persist a retryable 503: result=%+v finalized=%+v", sent, finalized)
		}
		if sent.Outcome == delivery.Acknowledged && finalized.Status != "succeeded" {
			t.Fatalf("notification did not succeed independently: result=%+v finalized=%+v", sent, finalized)
		}
	}
	if fulfillmentDeliveryID == "" {
		t.Fatal("neither fan-out delivery received the controlled fulfillment 503")
	}
	observed, err := c.getEvent(ctx, eventID)
	if err != nil {
		t.Fatal("read first persisted attempt:", err)
	}
	fulfillment, ok := observed.delivery("fulfillment")
	if !ok || fulfillment.Status != "pending" || !hasHTTPFailure(fulfillment, 503) {
		t.Fatalf("first 503 did not leave one pending retry: %+v", fulfillment)
	}
	notification, ok := observed.delivery("notification")
	if !ok || notification.Status != "succeeded" {
		t.Fatalf("notification did not succeed while fulfillment was unavailable: %+v", notification)
	}

	if err = c.recoverScenario(ctx, scenarioID, "fulfillment"); err != nil {
		t.Fatal("recover fulfillment:", err)
	}
	update, err := db.Pool.Exec(ctx, `UPDATE deliveries SET next_attempt_at=clock_timestamp() WHERE id=$1 AND status='pending' AND cycle_attempt_count=1`, fulfillmentDeliveryID)
	if err != nil || update.RowsAffected() != 1 {
		t.Fatalf("schedule the isolated pending retry in the test database: rows=%d err=%v", update.RowsAffected(), err)
	}
	claimed, err = claims.ClaimDue(ctx, 2)
	if err != nil || len(claimed) != 1 || claimed[0].DeliveryID != fulfillmentDeliveryID {
		t.Fatalf("recovery claims=%d err=%v, want the one pending fulfillment delivery", len(claimed), err)
	}
	sent := sender.Send(ctx, claimed[0])
	finalized, err := finalizer.Finalize(ctx, claimed[0], sent)
	if err != nil || sent.Outcome != delivery.Acknowledged || finalized.Status != "succeeded" {
		t.Fatalf("recovered fulfillment result=%+v finalized=%+v err=%v", sent, finalized, err)
	}

	final, err := c.getEvent(ctx, eventID)
	if err != nil {
		t.Fatal("read final event state:", err)
	}
	fulfillment, _ = final.delivery("fulfillment")
	notification, _ = final.delivery("notification")
	if fulfillment.Status != "succeeded" || fulfillment.CycleNo != 1 || fulfillment.LifetimeAttemptCount != 2 || notification.Status != "succeeded" || notification.LifetimeAttemptCount != 1 {
		t.Fatalf("retry-before-exhaustion state was not preserved: fulfillment=%+v notification=%+v", fulfillment, notification)
	}
	for _, code := range []string{"fulfillment", "notification"} {
		summaries, inspectErr := c.inspectScenario(ctx, scenarioID, code)
		wantRequests := map[string]int64{"fulfillment": 2, "notification": 1}[code]
		if inspectErr != nil || len(summaries) != 1 || summaries[0].RequestCount != wantRequests || summaries[0].EffectCount != 1 || len(summaries[0].OperationCounts) != 1 || summaries[0].OperationCounts[0].OperationID != map[string]string{"fulfillment": fulfillment.ID, "notification": notification.ID}[code] || summaries[0].OperationCounts[0].RequestCount != wantRequests || summaries[0].OperationCounts[0].EffectCount != 1 {
			t.Fatalf("persisted %s request/effect counts=%+v err=%v", code, summaries, inspectErr)
		}
	}
}
