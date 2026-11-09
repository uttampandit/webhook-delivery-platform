package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DemoRepository serializes each scenario with a PostgreSQL row lock. This is
// intentionally a small demo, not an independently scalable receiver service.
type DemoRepository struct {
	pool *pgxpool.Pool
	code string
}

func NewDemoRepository(pool *pgxpool.Pool, code string) *DemoRepository {
	return &DemoRepository{pool, code}
}
func (d *DemoRepository) Ping(ctx context.Context) error { return d.pool.Ping(ctx) }

type DemoOperationCount struct {
	OperationID  string `json:"operation_id"`
	RequestCount int64  `json:"request_count"`
	EffectCount  int64  `json:"effect_count"`
}
type DemoScenario struct {
	ScenarioID      string               `json:"scenario_id"`
	Mode            string               `json:"mode"`
	DedupeEnabled   bool                 `json:"dedupe_enabled"`
	DropRemaining   int                  `json:"drop_remaining"`
	RequestCount    int64                `json:"request_count"`
	EffectCount     int64                `json:"effect_count"`
	OperationCounts []DemoOperationCount `json:"operation_counts"`
}

func (d *DemoRepository) CreateScenario(ctx context.Context, id, mode string, dedupe bool) (bool, error) {
	if !ValidScenarioID(id) {
		return false, ErrInvalidInput
	}
	if !validDemoMode(mode) {
		return false, ErrDemoUnsupported
	}
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, ErrDemoUnavailable
	}
	defer tx.Rollback(ctx)
	drop := 0
	if mode == "commit_drop_once" {
		drop = 1
	}
	tag, err := tx.Exec(ctx, `INSERT INTO demo_scenarios(receiver_code,scenario_id,mode,dedupe_enabled,drop_remaining) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, d.code, id, mode, dedupe, drop)
	if err != nil {
		return false, ErrDemoUnavailable
	}
	var existingMode string
	var existingDedupe bool
	if err = tx.QueryRow(ctx, `SELECT mode,dedupe_enabled FROM demo_scenarios WHERE receiver_code=$1 AND scenario_id=$2 FOR UPDATE`, d.code, id).Scan(&existingMode, &existingDedupe); err != nil {
		return false, ErrDemoUnavailable
	}
	if mode != existingMode || dedupe != existingDedupe {
		return false, ErrDemoConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return false, ErrDemoUnavailable
	}
	return tag.RowsAffected() == 1, nil
}

// Inspect uses a consistent snapshot for all derived counts and bounds the
// per-operation response independently of the lifetime request count.
func (d *DemoRepository) Inspect(ctx context.Context, id string) (DemoScenario, error) {
	out := DemoScenario{ScenarioID: id, OperationCounts: []DemoOperationCount{}}
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, ErrDemoUnavailable
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `SELECT mode,dedupe_enabled,drop_remaining FROM demo_scenarios WHERE receiver_code=$1 AND scenario_id=$2`, d.code, id).Scan(&out.Mode, &out.DedupeEnabled, &out.DropRemaining)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrDemoNotFound
	}
	if err != nil {
		return out, ErrDemoUnavailable
	}
	err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM demo_requests WHERE receiver_code=$1 AND scenario_id=$2),(SELECT count(*) FROM demo_effects WHERE receiver_code=$1 AND scenario_id=$2)`, d.code, id).Scan(&out.RequestCount, &out.EffectCount)
	if err != nil {
		return out, ErrDemoUnavailable
	}
	rows, err := tx.Query(ctx, `SELECT r.operation_id::text,count(*),(SELECT count(*) FROM demo_effects e WHERE e.receiver_code=$1 AND e.scenario_id=$2 AND e.operation_id=r.operation_id) FROM demo_requests r WHERE r.receiver_code=$1 AND r.scenario_id=$2 GROUP BY r.operation_id ORDER BY r.operation_id LIMIT 100`, d.code, id)
	if err != nil {
		return out, ErrDemoUnavailable
	}
	for rows.Next() {
		var item DemoOperationCount
		if err = rows.Scan(&item.OperationID, &item.RequestCount, &item.EffectCount); err != nil {
			rows.Close()
			return out, ErrDemoUnavailable
		}
		out.OperationCounts = append(out.OperationCounts, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, ErrDemoUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return out, ErrDemoUnavailable
	}
	return out, nil
}

// Apply atomically persists ledger, effect and request. A commit error never
// produces an acknowledgment: the sender must treat that outcome as uncertain.
func (d *DemoRepository) Apply(ctx context.Context, operation string, e DemoEnvelope) (DemoResult, error) {
	empty := DemoResult{}
	if !e.valid || !isCanonicalUUID(operation) {
		return empty, ErrInvalidInput
	}
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return empty, ErrDemoUnavailable
	}
	defer tx.Rollback(ctx)
	var mode string
	var dedupe bool
	var drop int
	err = tx.QueryRow(ctx, `SELECT mode,dedupe_enabled,drop_remaining FROM demo_scenarios WHERE receiver_code=$1 AND scenario_id=$2 FOR UPDATE`, d.code, e.scenario).Scan(&mode, &dedupe, &drop)
	if errors.Is(err, pgx.ErrNoRows) {
		return empty, ErrDemoNotFound
	}
	if err != nil {
		return empty, ErrDemoUnavailable
	}
	if !validDemoMode(mode) {
		return empty, ErrDemoUnsupported
	}
	if mode == "http_503" || mode == "http_429" {
		outcome, status := "reject_503", 503
		if mode == "http_429" {
			outcome, status = "reject_429", 429
		}
		if _, err = tx.Exec(ctx, `INSERT INTO demo_requests(receiver_code,scenario_id,operation_id,planned_outcome,business_effect_added) VALUES($1,$2,$3,$4,false)`, d.code, e.scenario, operation, outcome); err != nil {
			return empty, ErrDemoUnavailable
		}
		if err = tx.Commit(ctx); err != nil {
			return empty, ErrDemoUnavailable
		}
		return DemoResult{HTTPStatus: status}, nil
	}
	added, conflict := true, false
	if dedupe {
		tag, err := tx.Exec(ctx, `INSERT INTO demo_dedup(receiver_code,scenario_id,operation_id,wire_hash) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, d.code, e.scenario, operation, e.hash[:])
		if err != nil {
			return empty, ErrDemoUnavailable
		}
		added = tag.RowsAffected() == 1
		if !added {
			var hash []byte
			if err = tx.QueryRow(ctx, `SELECT wire_hash FROM demo_dedup WHERE receiver_code=$1 AND scenario_id=$2 AND operation_id=$3`, d.code, e.scenario, operation).Scan(&hash); err != nil {
				return empty, ErrDemoUnavailable
			}
			conflict = !sameDemoHash(hash, e.hash)
		}
	}
	if added {
		_, effectID, err := newEventUUID()
		if err != nil {
			return empty, ErrDemoUnavailable
		}
		if _, err = tx.Exec(ctx, `INSERT INTO demo_effects(id,receiver_code,scenario_id,operation_id,order_id,quantity) VALUES($1,$2,$3,$4,$5,$6)`, effectID, d.code, e.scenario, operation, e.order, e.quantity); err != nil {
			return empty, ErrDemoUnavailable
		}
	}
	result := DemoResult{HTTPStatus: 204}
	outcome := "acknowledge"
	if !conflict && mode == "commit_drop_once" && drop == 1 {
		tag, err := tx.Exec(ctx, `UPDATE demo_scenarios SET drop_remaining=0,updated_at=clock_timestamp() WHERE receiver_code=$1 AND scenario_id=$2 AND drop_remaining=1`, d.code, e.scenario)
		if err != nil || tag.RowsAffected() != 1 {
			return empty, ErrDemoUnavailable
		}
		outcome = "drop"
		result.Drop = true
	}
	if conflict {
		outcome = "conflict"
	}
	if _, err = tx.Exec(ctx, `INSERT INTO demo_requests(receiver_code,scenario_id,operation_id,planned_outcome,business_effect_added) VALUES($1,$2,$3,$4,$5)`, d.code, e.scenario, operation, outcome, added); err != nil {
		return empty, ErrDemoUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return empty, ErrDemoUnavailable
	}
	if conflict {
		return empty, ErrDemoConflict
	}
	return result, nil
}

// DemoResult describes post-commit response intent, not proof of observation.
type DemoResult struct {
	HTTPStatus int
	Drop       bool
}

func validDemoMode(mode string) bool {
	switch mode {
	case "normal", "http_503", "http_429", "commit_drop_once":
		return true
	}
	return false
}

// RecoverScenario only permits an outage -> normal transition (or an
// idempotent normal -> normal). It never changes dedupe or rearms a drop.
func (d *DemoRepository) RecoverScenario(ctx context.Context, id string) error {
	if !ValidScenarioID(id) {
		return ErrInvalidInput
	}
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ErrDemoUnavailable
	}
	defer tx.Rollback(ctx)
	var mode string
	if err = tx.QueryRow(ctx, `SELECT mode FROM demo_scenarios WHERE receiver_code=$1 AND scenario_id=$2 FOR UPDATE`, d.code, id).Scan(&mode); errors.Is(err, pgx.ErrNoRows) {
		return ErrDemoNotFound
	} else if err != nil {
		return ErrDemoUnavailable
	}
	if mode != "normal" && mode != "http_503" && mode != "http_429" {
		return ErrInvalidInput
	}
	if _, err = tx.Exec(ctx, `UPDATE demo_scenarios SET mode='normal',updated_at=clock_timestamp() WHERE receiver_code=$1 AND scenario_id=$2`, d.code, id); err != nil {
		return ErrDemoUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return ErrDemoUnavailable
	}
	return nil
}
