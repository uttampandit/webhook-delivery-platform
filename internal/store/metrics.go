package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
)

// QueueMetricsRepository reads bounded, tenant-scoped status counts without
// locking delivery rows. Callers give it a dedicated small pool and timeout.
type QueueMetricsRepository struct {
	pool     *pgxpool.Pool
	tenantID string
}

func NewQueueMetricsRepository(pool *pgxpool.Pool, tenantID string) *QueueMetricsRepository {
	return &QueueMetricsRepository{pool: pool, tenantID: tenantID}
}

func (repository *QueueMetricsRepository) ReadQueueMetrics(ctx context.Context) (telemetry.GaugeSnapshot, error) {
	if repository == nil || repository.pool == nil || repository.tenantID == "" {
		return telemetry.GaugeSnapshot{}, ErrStoreUnavailable
	}
	var snapshot telemetry.GaugeSnapshot
	err := repository.pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE status='pending' AND next_attempt_at<=clock_timestamp() AND cycle_attempt_count<max_attempts),
			count(*) FILTER (WHERE status='in_flight'),
			count(*) FILTER (WHERE status='dead')
		FROM deliveries
		WHERE tenant_id=$1`, repository.tenantID).Scan(&snapshot.Due, &snapshot.InFlight, &snapshot.Dead)
	if err != nil {
		return telemetry.GaugeSnapshot{}, ErrStoreUnavailable
	}
	return snapshot, nil
}
