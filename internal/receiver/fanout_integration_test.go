//go:build integration

package receiver_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/receiver"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

func TestFanoutOutageRecoveryBeforeExhaustionPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, fulfillment, h := fixture(t, ctx)
	notification := store.NewDemoRepository(pool, "notification")
	if _, e := fulfillment.CreateScenario(ctx, "isolated", "http_503", true); e != nil {
		t.Fatal(e)
	}
	if _, e := notification.CreateScenario(ctx, "isolated", "normal", true); e != nil {
		t.Fatal(e)
	}
	fHTTP := httptest.NewServer(h.Webhook())
	defer fHTTP.Close()
	nHTTP := httptest.NewServer(receiver.New(notification, signing.Auth{Keys: keys{}}, publicToken, time.Second).Webhook())
	defer nHTTP.Close()
	controls := httptest.NewServer(h.Control())
	defer controls.Close()
	registry, e := config.RegistryForMode(config.RunModeCompose)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Seed(ctx, pool, registry); e != nil {
		t.Fatal(e)
	}
	for code, url := range map[string]string{"fulfillment": fHTTP.URL, "notification": nHTTP.URL} {
		if _, e = pool.Exec(ctx, `UPDATE destinations SET url=$1,key_ref=$2 WHERE code=$3`, url+"/webhook", "public-test-v1", code); e != nil {
			t.Fatal(e)
		}
	}
	req, e := store.ValidateEventRequest([]byte(fmt.Sprintf(`{"type":"order.confirmed","payload":{"scenario_id":"isolated","order_id":"order-1","quantity":1},"destination_ids":[%q,%q]}`, config.FulfillmentDestination, config.NotificationDestination)))
	if e != nil {
		t.Fatal(e)
	}
	event, e := store.NewEventRepository(pool).AcceptEvent(ctx, config.TenantID, "fanout-recovery", req)
	if e != nil {
		t.Fatal(e)
	}
	claims, e := store.NewClaimRepository(pool, 30*time.Second, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	finalizer, e := store.NewFinalizationRepository(pool, time.Second, delivery.Policy{})
	if e != nil {
		t.Fatal(e)
	}
	sender := sender(t, signing.Auth{Keys: keys{}})
	worker, e := delivery.NewWorker(claims, finalizer, sender, delivery.WorkerOptions{Concurrency: 2, PollInterval: 10 * time.Millisecond, FinalizeTimeout: time.Second, ShutdownTimeout: 2 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	running, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(running) }()
	defer func() {
		stop()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(3 * time.Second):
			t.Error("worker did not stop")
		}
	}()
	var last string
	await := func(predicate func() bool) {
		t.Helper()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			if predicate() {
				return
			}
			select {
			case <-tick.C:
			case <-ctx.Done():
				t.Fatalf("fanout deadline (last: %s)", last)
			}
		}
	}
	// Observe finalized rejection and independent healthy completion before recovery.
	await(func() bool {
		var ready bool
		e = pool.QueryRow(ctx, `SELECT bool_and(CASE WHEN destination_id=$2 THEN status='pending' AND cycle_attempt_count>=1 WHEN destination_id=$3 THEN status='succeeded' ELSE false END),string_agg(status||':'||cycle_attempt_count::text,',' ORDER BY destination_id) FROM deliveries WHERE event_id=$1`, event.EventID, config.FulfillmentDestination, config.NotificationDestination).Scan(&ready, &last)
		if e != nil {
			t.Fatal(e)
		}
		return ready
	})
	var ms int64
	var next, finished time.Time
	if e = pool.QueryRow(ctx, `SELECT a.retry_delay_ms,a.finished_at,d.next_attempt_at FROM deliveries d JOIN attempts a ON a.delivery_id=d.id WHERE d.event_id=$1 AND d.destination_id=$2 AND a.attempt_no=1`, event.EventID, config.FulfillmentDestination).Scan(&ms, &finished, &next); e != nil {
		t.Fatal(e)
	}
	if ms != 2000 || next.Sub(finished) != 2*time.Second {
		t.Fatal("Retry-After was not honored")
	}
	control(t, ctx, controls.URL, "PATCH", "/control/scenarios/isolated", `{"mode":"normal"}`, 200)
	await(func() bool {
		var ready bool
		e = pool.QueryRow(ctx, `SELECT bool_and(status='succeeded'),string_agg(status||':'||cycle_attempt_count::text,',' ORDER BY destination_id) FROM deliveries WHERE event_id=$1`, event.EventID).Scan(&ready, &last)
		if e != nil {
			t.Fatal(e)
		}
		return ready
	})
	f, e := fulfillment.Inspect(ctx, "isolated")
	if e != nil {
		t.Fatal(e)
	}
	n, e := notification.Inspect(ctx, "isolated")
	if e != nil {
		t.Fatal(e)
	}
	if f.RequestCount != 2 || f.EffectCount != 1 || n.RequestCount != 1 || n.EffectCount != 1 {
		t.Fatalf("requests/effects fulfillment=%d/%d notification=%d/%d", f.RequestCount, f.EffectCount, n.RequestCount, n.EffectCount)
	}
	var attempts int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM attempts a JOIN deliveries d ON d.id=a.delivery_id WHERE d.event_id=$1`, event.EventID).Scan(&attempts); e != nil || attempts != 3 {
		t.Fatalf("attempts=%d error=%v", attempts, e)
	}
}
