package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInvalidState = errors.New("delivery cannot be replayed in its current state")

type ReplayResult struct {
	ReplayID     string `json:"replay_id"`
	DeliveryID   string `json:"delivery_id"`
	CycleNo      int    `json:"cycle_no"`
	Deduplicated bool   `json:"deduplicated"`
}

type ReplayRepository struct{ pool *pgxpool.Pool }

func NewReplayRepository(pool *pgxpool.Pool) *ReplayRepository { return &ReplayRepository{pool} }

// Replay schedules the same operation in a fresh cycle, after persisting its
// audit row. READ COMMITTED rechecks observe winners committed during waits.
func (r *ReplayRepository) Replay(ctx context.Context, tenantID, deliveryID, key, actor string) (ReplayResult, error) {
	empty := ReplayResult{}
	if r == nil || r.pool == nil {
		return empty, ErrStoreUnavailable
	}
	if !isCanonicalUUID(tenantID) || !isCanonicalUUID(deliveryID) || ValidateIdempotencyKey(key) != nil || actor != "local-operator" {
		return empty, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return empty, ErrStoreUnavailable
	}
	defer tx.Rollback(ctx)
	existing := func() (ReplayResult, error) {
		var result ReplayResult
		err := tx.QueryRow(ctx, `SELECT id::text,delivery_id::text,to_cycle FROM replays WHERE tenant_id=$1 AND request_key=$2`, tenantID, key).Scan(&result.ReplayID, &result.DeliveryID, &result.CycleNo)
		if errors.Is(err, pgx.ErrNoRows) {
			return empty, pgx.ErrNoRows
		}
		if err != nil {
			return empty, ErrStoreUnavailable
		}
		if result.DeliveryID != deliveryID {
			return empty, ErrIdempotencyConflict
		}
		result.Deduplicated = true
		return result, nil
	}
	if result, err := existing(); !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	var status string
	var cycle int
	err = tx.QueryRow(ctx, `SELECT status,cycle_no FROM deliveries WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, deliveryID).Scan(&status, &cycle)
	if errors.Is(err, pgx.ErrNoRows) {
		return empty, ErrNotFound
	}
	if err != nil {
		return empty, ErrStoreUnavailable
	}
	if result, err := existing(); !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	if status != "dead" {
		return empty, ErrInvalidState
	}
	id, _, err := newEventUUID()
	if err != nil {
		return empty, ErrStoreUnavailable
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return empty, ErrStoreUnavailable
	}
	var inserted string
	err = tx.QueryRow(ctx, `INSERT INTO replays(id,tenant_id,delivery_id,request_key,actor,from_cycle,to_cycle,created_at) VALUES($1,$2,$3,$4,$5,$6,$6+1,$7) ON CONFLICT (tenant_id,request_key) DO NOTHING RETURNING id::text`, id, tenantID, deliveryID, key, actor, cycle, now).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return existing()
	}
	if err != nil {
		return empty, ErrStoreUnavailable
	}
	tag, err := tx.Exec(ctx, `UPDATE deliveries SET status='pending',cycle_no=cycle_no+1,cycle_attempt_count=0,generation=generation+1,next_attempt_at=$3,updated_at=$3,terminal_reason=NULL,claim_token=NULL,lease_until=NULL WHERE tenant_id=$1 AND id=$2 AND status='dead'`, tenantID, deliveryID, now)
	if err != nil || tag.RowsAffected() != 1 {
		return empty, ErrStoreUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return empty, ErrAmbiguousCommit
	}
	return ReplayResult{ReplayID: inserted, DeliveryID: deliveryID, CycleNo: cycle + 1}, nil
}
