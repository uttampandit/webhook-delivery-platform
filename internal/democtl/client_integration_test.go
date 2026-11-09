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
