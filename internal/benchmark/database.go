package benchmark

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

func openDatabase(ctx context.Context, settings config.Settings) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(settings.DatabaseURL())
	if err != nil {
		return nil, errors.New("benchmark PostgreSQL connection could not be configured")
	}
	poolConfig.MaxConns = benchmarkDatabasePoolMax
	poolConfig.MinConns = 0
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = settings.PostgreSQLStatementTimeout()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.New("benchmark PostgreSQL connection could not be created")
	}
	return pool, nil
}

func databaseIdentity(ctx context.Context, pool *pgxpool.Pool) (DatabaseMetadata, error) {
	var result DatabaseMetadata
	if err := pool.QueryRow(ctx, `SELECT current_database(), current_setting('server_version')`).Scan(&result.Name, &result.ServerVersion); err != nil {
		return DatabaseMetadata{}, errors.New("benchmark PostgreSQL identity query failed")
	}
	return result, nil
}

func ensureUnusedRun(ctx context.Context, pool *pgxpool.Pool, tenantID, scenarioID string, keys []string) error {
	var scenarioExists, eventExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM demo_scenarios WHERE scenario_id=$1)`, scenarioID).Scan(&scenarioExists); err != nil {
		return errors.New("benchmark run identity query failed")
	}
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE tenant_id=$1 AND idempotency_key=ANY($2::text[]))`, tenantID, keys).Scan(&eventExists); err != nil {
		return errors.New("benchmark run identity query failed")
	}
	if scenarioExists || eventExists {
		return errors.New("benchmark run identity already exists; choose a new run-id")
	}
	return nil
}

type deliveryProgressCounts struct {
	pending  int
	inFlight int
}

func deliveryProgress(ctx context.Context, pool *pgxpool.Pool, eventIDs []string) (deliveryProgressCounts, error) {
	var result deliveryProgressCounts
	if len(eventIDs) == 0 {
		return result, nil
	}
	err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status='pending')::int,
		       count(*) FILTER (WHERE status='in_flight')::int
		FROM deliveries
		WHERE event_id::text=ANY($1::text[])`, eventIDs).Scan(&result.pending, &result.inFlight)
	if err != nil {
		return deliveryProgressCounts{}, errors.New("benchmark delivery progress query failed")
	}
	return result, nil
}

func collectSnapshot(ctx context.Context, pool *pgxpool.Pool, tenantID, scenarioID string, keys []string) (snapshot, error) {
	result := snapshot{Receivers: map[string]receiverRow{}}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return snapshot{}, errors.New("benchmark reconciliation transaction could not start")
	}
	defer tx.Rollback(ctx)
	if err = tx.QueryRow(ctx, `SELECT current_database(), current_setting('server_version')`).Scan(&result.DatabaseName, &result.DBVersion); err != nil {
		return snapshot{}, errors.New("benchmark PostgreSQL identity query failed")
	}
	eventRows, err := tx.Query(ctx, `
		SELECT id::text, idempotency_key, created_at,
		       octet_length(request_body)::int, octet_length(wire_body)::int
		FROM events
		WHERE tenant_id=$1 AND idempotency_key=ANY($2::text[])
		ORDER BY idempotency_key`, tenantID, keys)
	if err != nil {
		return snapshot{}, errors.New("benchmark event reconciliation query failed")
	}
	for eventRows.Next() {
		var row eventRow
		if err = eventRows.Scan(&row.ID, &row.IdempotencyKey, &row.CreatedAt, &row.RequestBytes, &row.WireBytes); err != nil {
			eventRows.Close()
			return snapshot{}, errors.New("benchmark event reconciliation scan failed")
		}
		result.Events = append(result.Events, row)
	}
	err = eventRows.Err()
	eventRows.Close()
	if err != nil {
		return snapshot{}, errors.New("benchmark event reconciliation query failed")
	}

	ids := eventIDsOf(result.Events)
	if len(ids) > 0 {
		deliveryRows, queryErr := tx.Query(ctx, `
			SELECT d.id::text, e.id::text, dest.code, d.status, e.created_at,
			       (SELECT min(a.started_at) FROM attempts a WHERE a.delivery_id=d.id),
			       d.confirmed_at,
			       (SELECT count(*)::int FROM attempts a WHERE a.delivery_id=d.id)
			FROM events e
		JOIN deliveries d ON d.tenant_id=e.tenant_id AND d.event_id=e.id
		JOIN destinations dest ON dest.tenant_id=d.tenant_id AND dest.id=d.destination_id
		WHERE e.tenant_id=$1 AND e.id::text=ANY($2::text[])
		ORDER BY dest.code, e.id, d.id`, tenantID, ids)
		if queryErr != nil {
			return snapshot{}, errors.New("benchmark delivery reconciliation query failed")
		}
		for deliveryRows.Next() {
			var row deliveryRow
			if err = deliveryRows.Scan(&row.ID, &row.EventID, &row.Destination, &row.Status, &row.CreatedAt, &row.FirstStart, &row.ConfirmedAt, &row.Attempts); err != nil {
				deliveryRows.Close()
				return snapshot{}, errors.New("benchmark delivery reconciliation scan failed")
			}
			result.Deliveries = append(result.Deliveries, row)
		}
		err = deliveryRows.Err()
		deliveryRows.Close()
		if err != nil {
			return snapshot{}, errors.New("benchmark delivery reconciliation query failed")
		}
	}

	scenarioRows, err := tx.Query(ctx, `
		SELECT receiver_code, mode, dedupe_enabled
		FROM demo_scenarios WHERE scenario_id=$1 ORDER BY receiver_code`, scenarioID)
	if err != nil {
		return snapshot{}, errors.New("benchmark receiver scenario reconciliation query failed")
	}
	for scenarioRows.Next() {
		var row receiverRow
		if err = scenarioRows.Scan(&row.Code, &row.Mode, &row.Dedupe); err != nil {
			scenarioRows.Close()
			return snapshot{}, errors.New("benchmark receiver scenario reconciliation scan failed")
		}
		row.RequestOps = []string{}
		row.EffectOps = []string{}
		result.Receivers[row.Code] = row
	}
	err = scenarioRows.Err()
	scenarioRows.Close()
	if err != nil {
		return snapshot{}, errors.New("benchmark receiver scenario reconciliation query failed")
	}
	for _, code := range []string{"fulfillment", "notification"} {
		row, exists := result.Receivers[code]
		if !exists {
			continue
		}
		if err = tx.QueryRow(ctx, `
			SELECT count(*)::bigint, count(DISTINCT operation_id)::bigint
			FROM demo_requests WHERE receiver_code=$1 AND scenario_id=$2`, code, scenarioID).
			Scan(&row.Requests, &row.RequestOperationCount); err != nil {
			return snapshot{}, errors.New("benchmark receiver request reconciliation query failed")
		}
		row.RequestOps, err = scenarioOperations(ctx, tx, code, scenarioID, "demo_requests")
		if err != nil {
			return snapshot{}, err
		}
		if err = tx.QueryRow(ctx, `
			SELECT count(*)::bigint, count(DISTINCT operation_id)::bigint
			FROM demo_effects WHERE receiver_code=$1 AND scenario_id=$2`, code, scenarioID).
			Scan(&row.Effects, &row.EffectOperationCount); err != nil {
			return snapshot{}, errors.New("benchmark receiver effect reconciliation query failed")
		}
		row.EffectOps, err = scenarioOperations(ctx, tx, code, scenarioID, "demo_effects")
		if err != nil {
			return snapshot{}, err
		}
		result.Receivers[code] = row
	}
	if err = tx.Commit(ctx); err != nil {
		return snapshot{}, errors.New("benchmark reconciliation transaction failed")
	}
	return result, nil
}

func scenarioOperations(ctx context.Context, tx pgx.Tx, code, scenarioID, table string) ([]string, error) {
	if table != "demo_requests" && table != "demo_effects" {
		return nil, errors.New("benchmark scenario query is invalid")
	}
	query := `SELECT DISTINCT operation_id::text FROM ` + table + ` WHERE receiver_code=$1 AND scenario_id=$2 ORDER BY operation_id`
	rows, err := tx.Query(ctx, query, code, scenarioID)
	if err != nil {
		return nil, errors.New("benchmark receiver operation reconciliation query failed")
	}
	defer rows.Close()
	items := []string{}
	for rows.Next() {
		var operation string
		if err = rows.Scan(&operation); err != nil {
			return nil, errors.New("benchmark receiver operation reconciliation scan failed")
		}
		items = append(items, operation)
	}
	if err = rows.Err(); err != nil {
		return nil, errors.New("benchmark receiver operation reconciliation query failed")
	}
	return items, nil
}
