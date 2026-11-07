package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

// DestinationSummary contains only the fields safe for the operator API.
type DestinationSummary struct {
	ID   string
	Code string
}

// APIReadRepository contains the bounded database operations required by the
// API health, fixed-destination, event-acceptance, and event-query endpoints.
type APIReadRepository struct {
	pool    *pgxpool.Pool
	events  *EventRepository
	queries *QueryRepository
}

func NewAPIReadRepository(pool *pgxpool.Pool) *APIReadRepository {
	return &APIReadRepository{
		pool:    pool,
		events:  NewEventRepository(pool),
		queries: NewQueryRepository(pool),
	}
}

// AcceptEvent delegates bounded API writes to the atomic event repository.
func (repository *APIReadRepository) AcceptEvent(ctx context.Context, tenantID, key string, request ValidatedEventRequest) (AcceptedEvent, error) {
	if repository == nil || repository.events == nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	return repository.events.AcceptEvent(ctx, tenantID, key, request)
}

// ListEvents delegates tenant-scoped event list queries to the QueryRepository.
func (repository *APIReadRepository) ListEvents(ctx context.Context, tenantID string, limit int, cursor *EventCursor) ([]EventListItem, *EventCursor, error) {
	if repository == nil || repository.queries == nil {
		return nil, nil, ErrStoreUnavailable
	}
	return repository.queries.ListEvents(ctx, tenantID, limit, cursor)
}

// GetEvent delegates tenant-scoped event detail queries to the QueryRepository.
func (repository *APIReadRepository) GetEvent(ctx context.Context, tenantID, eventID string) (EventDetail, error) {
	if repository == nil || repository.queries == nil {
		return EventDetail{}, ErrStoreUnavailable
	}
	return repository.queries.GetEvent(ctx, tenantID, eventID)
}

func (repository *APIReadRepository) Ping(ctx context.Context) error {
	if repository == nil || repository.pool == nil {
		return errors.New("database unavailable")
	}
	if err := repository.pool.Ping(ctx); err != nil {
		return errors.New("database unavailable")
	}
	return nil
}

// CheckReadiness verifies migration version 1, the fixed tenant, and exactly
// the two immutable destinations selected by the current run mode.
func (repository *APIReadRepository) CheckReadiness(ctx context.Context, registry config.Registry) error {
	if repository == nil || repository.pool == nil {
		return errors.New("database unavailable")
	}
	tenant := registry.Tenant()
	destinations := registry.Destinations()
	if len(destinations) != 2 || destinations[0].TenantID != tenant.ID || destinations[1].TenantID != tenant.ID {
		return errors.New("configured registry is incomplete")
	}

	var migrationReady, tenantReady, destinationCountReady, firstDestinationReady, secondDestinationReady bool
	err := repository.pool.QueryRow(ctx, `
		SELECT
			EXISTS (
				SELECT 1 FROM schema_migrations
				WHERE version = $1 AND name = $2
			),
			EXISTS (
				SELECT 1 FROM tenants WHERE id = $3 AND name = $4
			),
			(SELECT count(*) FROM destinations WHERE tenant_id = $3) = 2,
			EXISTS (
				SELECT 1 FROM destinations
				WHERE tenant_id = $3 AND id = $5 AND code = $6 AND url = $7 AND key_ref = $8
			),
			EXISTS (
				SELECT 1 FROM destinations
				WHERE tenant_id = $3 AND id = $9 AND code = $10 AND url = $11 AND key_ref = $12
			)`,
		int64(1), "001_initial.sql", tenant.ID, tenant.Name,
		destinations[0].ID, destinations[0].Code, destinations[0].URL, destinations[0].KeyRef,
		destinations[1].ID, destinations[1].Code, destinations[1].URL, destinations[1].KeyRef,
	).Scan(&migrationReady, &tenantReady, &destinationCountReady, &firstDestinationReady, &secondDestinationReady)
	if err != nil {
		return errors.New("schema or seed readiness check failed")
	}
	if !migrationReady || !tenantReady || !destinationCountReady || !firstDestinationReady || !secondDestinationReady {
		return errors.New("schema or seed is not ready")
	}
	return nil
}

func (repository *APIReadRepository) ListDestinations(ctx context.Context, tenantID string) ([]DestinationSummary, error) {
	if repository == nil || repository.pool == nil {
		return nil, errors.New("database unavailable")
	}
	rows, err := repository.pool.Query(ctx, `
		SELECT id::text, code
		FROM destinations
		WHERE tenant_id = $1
		ORDER BY code`, tenantID)
	if err != nil {
		return nil, errors.New("destination query failed")
	}
	defer rows.Close()

	items := make([]DestinationSummary, 0, 2)
	for rows.Next() {
		var item DestinationSummary
		if err := rows.Scan(&item.ID, &item.Code); err != nil {
			return nil, errors.New("destination query failed")
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("destination query failed")
	}
	return items, nil
}
