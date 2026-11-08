package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
)

var ErrRecoveryUnavailable = errors.New("recovery store unavailable")
var ErrRecoveryCommitUnknown = errors.New("recovery commit outcome is unknown")

type RecoveryRepository struct {
	pool    *pgxpool.Pool
	timeout time.Duration
	policy  delivery.Policy
}

func NewRecoveryRepository(pool *pgxpool.Pool, timeout time.Duration, policy delivery.Policy) (*RecoveryRepository, error) {
	if pool == nil || timeout <= 0 {
		return nil, ErrInvalidInput
	}
	return &RecoveryRepository{pool, timeout, policy}, nil
}

// RecoverExpired closes at most 100 abandoned attempts atomically. No HTTP or
// application clock determines expiry, and a failed batch exposes no count.
func (r *RecoveryRepository) RecoverExpired(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, ErrRecoveryUnavailable
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id::text FROM deliveries WHERE status='in_flight' AND lease_until<=clock_timestamp() ORDER BY lease_until,id LIMIT 100 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return 0, ErrRecoveryUnavailable
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return 0, ErrRecoveryUnavailable
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, ErrRecoveryUnavailable
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return 0, ErrRecoveryUnavailable
	}
	count := 0
	for _, id := range ids {
		var generation int64
		var token string
		var cycle, lifetime, cycleNo int
		var lease time.Time
		if err = tx.QueryRow(ctx, `SELECT generation,claim_token::text,cycle_attempt_count,lifetime_attempt_count,cycle_no,lease_until FROM deliveries WHERE id=$1 AND status='in_flight'`, id).Scan(&generation, &token, &cycle, &lifetime, &cycleNo, &lease); err != nil {
			return 0, ErrRecoveryUnavailable
		}
		if lease.After(now) {
			continue
		}
		var attemptID string
		var started time.Time
		err = tx.QueryRow(ctx, `SELECT id::text,started_at FROM attempts WHERE delivery_id=$1 AND generation=$2 AND claim_token=$3 AND outcome='running' AND attempt_no=$4 AND cycle_attempt_no=$5 AND cycle_no=$6`, id, generation, token, lifetime, cycle, cycleNo).Scan(&attemptID, &started)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrAttemptInvariant
		}
		if err != nil {
			return 0, ErrRecoveryUnavailable
		}
		decision, err := r.policy.Decide(delivery.SendResult{Outcome: delivery.Unconfirmed, ErrorCode: "lease_expired"}, cycle, now)
		if err != nil {
			return 0, err
		}
		duration := now.Sub(started).Milliseconds()
		if duration < 0 {
			duration = 0
		}
		var delay *int64
		var next *time.Time
		var reason *string
		if decision.Status == "pending" {
			ms := decision.RetryDelay.Milliseconds()
			delay = &ms
			due := now.Add(decision.RetryDelay)
			next = &due
		}
		if decision.TerminalReason != "" {
			reason = &decision.TerminalReason
		}
		tag, err := tx.Exec(ctx, `UPDATE attempts SET outcome='unconfirmed',error_code='lease_expired',finished_at=$2,duration_ms=$3,retry_delay_ms=$4 WHERE id=$1 AND outcome='running'`, attemptID, now, duration, delay)
		if err != nil {
			return 0, ErrRecoveryUnavailable
		}
		if tag.RowsAffected() != 1 {
			return 0, ErrAttemptInvariant
		}
		_, err = tx.Exec(ctx, `UPDATE deliveries SET generation=generation+1,claim_token=NULL,lease_until=NULL,status=$2,terminal_reason=$3,next_attempt_at=COALESCE($4::timestamptz,next_attempt_at),updated_at=$5 WHERE id=$1`, id, decision.Status, reason, next, now)
		if err != nil {
			return 0, ErrRecoveryUnavailable
		}
		count++
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, ErrRecoveryCommitUnknown
	}
	return count, nil
}
