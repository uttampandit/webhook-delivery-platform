//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestQueueMetricsRepositoryPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	applyEventSchema(t, ctx, database.Pool)
	registry, err := config.RegistryForMode(config.RunModeCompose)
	if err != nil {
		t.Fatal(err)
	}
	if err := Seed(ctx, database.Pool, registry); err != nil {
		t.Fatal(err)
	}
	repository := NewQueueMetricsRepository(database.Pool, registry.Tenant().ID)
	empty, err := repository.ReadQueueMetrics(ctx)
	if err != nil || empty.Due != 0 || empty.InFlight != 0 || empty.Dead != 0 {
		t.Fatalf("empty queue metrics=%+v err=%v", empty, err)
	}
	request, err := ValidateEventRequest(eventRequestBody(registry.Destinations()[0].ID, registry.Destinations()[1].ID))
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewEventRepository(database.Pool).AcceptEvent(ctx, registry.Tenant().ID, "metrics-fixture", request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `UPDATE deliveries SET next_attempt_at='2000-01-01T00:00:00Z' WHERE tenant_id=$1`, registry.Tenant().ID); err != nil {
		t.Fatal(err)
	}
	claims, err := NewClaimRepository(database.Pool, 30*time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := claims.ClaimDue(ctx, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%d err=%v", len(claimed), err)
	}
	if claimed[0].ScheduledAt.IsZero() || claimed[0].EventCreatedAt.IsZero() {
		t.Fatalf("claim metrics timestamps not loaded: %+v", claimed[0])
	}
	if _, err := database.Pool.Exec(ctx, `UPDATE deliveries SET status='dead',terminal_reason='max_attempts' WHERE tenant_id=$1 AND id<>$2`, registry.Tenant().ID, claimed[0].DeliveryID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.ReadQueueMetrics(ctx)
	if err != nil || snapshot.Due != 0 || snapshot.InFlight != 1 || snapshot.Dead != 1 {
		t.Fatalf("scoped queue metrics=%+v err=%v", snapshot, err)
	}
	status := 204
	finalizer, err := NewFinalizationRepository(database.Pool, time.Second, delivery.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	finalized, err := finalizer.Finalize(ctx, claimed[0], delivery.SendResult{Outcome: delivery.Acknowledged, HTTPStatus: &status})
	if err != nil || finalized.Status != "succeeded" || finalized.Outcome != delivery.Acknowledged || !finalized.DeliveryLatencyObserved || finalized.DeliveryLatency < 0 {
		t.Fatalf("first acknowledgment latency=%+v err=%v", finalized, err)
	}
	snapshot, err = repository.ReadQueueMetrics(ctx)
	if err != nil || snapshot.Due != 0 || snapshot.InFlight != 0 || snapshot.Dead != 1 {
		t.Fatalf("terminal queue metrics=%+v err=%v", snapshot, err)
	}
	otherTenant, err := NewQueueMetricsRepository(database.Pool, "00000000-0000-4000-8000-000000000099").ReadQueueMetrics(ctx)
	if err != nil || otherTenant != (telemetry.GaugeSnapshot{}) {
		t.Fatalf("queue metrics were not tenant scoped: %+v err=%v", otherTenant, err)
	}
}
