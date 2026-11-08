//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestReplayHTTPPostgreSQLLostResponseAndTenantScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, e := testutil.NewDatabase(ctx, t)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Apply(ctx, db.Pool, testMigrationDir(t)); e != nil {
		t.Fatal(e)
	}
	settings := testAPISettings(t)
	if e = store.Seed(ctx, db.Pool, settings.Registry()); e != nil {
		t.Fatal(e)
	}
	request, e := store.ValidateEventRequest([]byte(fmt.Sprintf(`{"type":"order.confirmed","payload":{},"destination_ids":["%s"]}`, config.FulfillmentDestination)))
	if e != nil {
		t.Fatal(e)
	}
	// Ingest and replay keys use independent namespaces.
	accepted, e := store.NewEventRepository(db.Pool).AcceptEvent(ctx, config.TenantID, "same-literal-key", request)
	if e != nil {
		t.Fatal(e)
	}
	id := accepted.DeliveryIDs[0]
	claims, e := store.NewClaimRepository(db.Pool, 30*time.Second, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	finalizer, e := store.NewFinalizationRepository(db.Pool, time.Second, delivery.Policy{})
	if e != nil {
		t.Fatal(e)
	}
	cs, e := claims.ClaimDue(ctx, 1)
	if e != nil || len(cs) != 1 {
		t.Fatal(e)
	}
	code := 400
	if _, e = finalizer.Finalize(ctx, cs[0], delivery.SendResult{Outcome: delivery.HTTPFailure, HTTPStatus: &code}); e != nil {
		t.Fatal(e)
	}
	handler := NewHandler(settings, store.NewAPIReadRepository(db.Pool))
	newRequest := func(key string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := replayRequest("{}", key)
		r.URL.Path = "/v1/deliveries/" + id + "/replay"
		handler.ServeHTTP(w, r)
		return w
	}
	lost := &disconnectedEventResponse{header: make(map[string][]string)}
	r := replayRequest("{}", "same-literal-key")
	r.URL.Path = "/v1/deliveries/" + id + "/replay"
	handler.ServeHTTP(lost, r)
	if lost.status != 202 {
		t.Fatal(lost.status)
	}
	duplicate := newRequest("same-literal-key")
	if duplicate.Code != 200 {
		t.Fatal(duplicate.Code, duplicate.Body.String())
	}
	var original store.ReplayResult
	if e = json.Unmarshal(duplicate.Body.Bytes(), &original); e != nil {
		t.Fatal(e)
	}
	if !original.Deduplicated || original.CycleNo != 2 || original.DeliveryID != id {
		t.Fatal(original)
	}
	assertCreateEventError(t, newRequest("new-key"), 409, codeInvalidState)
	cs, e = claims.ClaimDue(ctx, 1)
	if e != nil || len(cs) != 1 {
		t.Fatal(e)
	}
	assertCreateEventError(t, newRequest("inflight-key"), 409, codeInvalidState)
	if _, e = finalizer.Finalize(ctx, cs[0], delivery.SendResult{Outcome: delivery.Acknowledged, HTTPStatus: statusPointer(204)}); e != nil {
		t.Fatal(e)
	}
	assertCreateEventError(t, newRequest("success-key"), 409, codeInvalidState)
	after := newRequest("same-literal-key")
	if after.Code != 200 || after.Body.String() != duplicate.Body.String() {
		t.Fatal("old response changed", after.Body.String())
	}
	var actor string
	var count int
	if e = db.Pool.QueryRow(ctx, `SELECT actor,(SELECT count(*) FROM replays) FROM replays WHERE id=$1`, original.ReplayID).Scan(&actor, &count); e != nil || actor != "local-operator" || count != 1 {
		t.Fatal(actor, count, e)
	}
	// A genuine foreign delivery must be indistinguishable from an unknown ID.
	const foreignTenant = "00000000-0000-4000-8000-000000000099"
	const foreignDestination = "00000000-0000-4000-8000-000000000098"
	if _, e = db.Pool.Exec(ctx, `WITH tenant AS (INSERT INTO tenants(id,name) VALUES($1,'foreign') RETURNING id) INSERT INTO destinations(id,tenant_id,code,url,key_ref) SELECT $2,id,'foreign','http://127.0.0.1/foreign','foreign' FROM tenant`, foreignTenant, foreignDestination); e != nil {
		t.Fatal(e)
	}
	foreignRequest, e := store.ValidateEventRequest([]byte(fmt.Sprintf(`{"type":"order.confirmed","payload":{},"destination_ids":["%s"]}`, foreignDestination)))
	if e != nil {
		t.Fatal(e)
	}
	foreign, e := store.NewEventRepository(db.Pool).AcceptEvent(ctx, foreignTenant, "foreign-key", foreignRequest)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Pool.Exec(ctx, `UPDATE deliveries SET status='dead',terminal_reason='permanent_http' WHERE id=$1`, foreign.DeliveryIDs[0]); e != nil {
		t.Fatal(e)
	}

	foreignReplay, e := store.NewReplayRepository(db.Pool).Replay(ctx, foreignTenant, foreign.DeliveryIDs[0], "same-literal-key", "local-operator")
	if e != nil || foreignReplay.Deduplicated || foreignReplay.ReplayID == original.ReplayID {
		t.Fatal("tenant key namespace", foreignReplay, e)
	}
	if own := newRequest("same-literal-key"); own.Code != 200 || own.Body.String() != duplicate.Body.String() {
		t.Fatal("foreign key changed own response")
	}
	for _, target := range []string{foreign.DeliveryIDs[0], "00000000-0000-4000-8000-000000000097"} {
		w := httptest.NewRecorder()
		r := replayRequest("{}", "foreign-replay-key")
		r.URL.Path = "/v1/deliveries/" + target + "/replay"
		handler.ServeHTTP(w, r)
		assertCreateEventError(t, w, 404, codeNotFound)
	}
	if e = db.Pool.QueryRow(ctx, `SELECT count(*) FROM replays WHERE tenant_id=$1`, foreignTenant).Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
}

func statusPointer(code int) *int { return &code }
