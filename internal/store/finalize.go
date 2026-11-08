package store

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
)

var (
	ErrStaleClaim            = errors.New("stale delivery claim")
	ErrFinalizeUnavailable   = errors.New("finalization store unavailable")
	ErrFinalizeCommitUnknown = errors.New("finalization commit outcome is unknown")
	ErrAttemptInvariant      = errors.New("matching running attempt is missing")
)

type FinalizationRepository struct {
	pool    *pgxpool.Pool
	timeout time.Duration
	policy  delivery.Policy
}

func NewFinalizationRepository(pool *pgxpool.Pool, timeout time.Duration, policy delivery.Policy) (*FinalizationRepository, error) {
	if pool == nil || timeout <= 0 {
		return nil, ErrInvalidInput
	}
	return &FinalizationRepository{pool, timeout, policy}, nil
}
func (r *FinalizationRepository) Finalize(ctx context.Context, c delivery.ClaimedDelivery, result delivery.SendResult) (delivery.FinalizationResult, error) {
	empty := delivery.FinalizationResult{}
	if !isCanonicalUUID(c.DeliveryID) || !isCanonicalUUID(c.AttemptID) || !isCanonicalUUID(c.ClaimToken) || c.Generation < 1 {
		return empty, ErrInvalidInput
	}
	normalized, err := delivery.Normalize(result)
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return empty, ErrFinalizeUnavailable
	}
	defer tx.Rollback(ctx)
	var status string
	var generation int64
	var token pgtype.UUID
	var lease pgtype.Timestamptz
	var count int
	err = tx.QueryRow(ctx, `SELECT status,generation,claim_token,lease_until,cycle_attempt_count FROM deliveries WHERE id=$1 FOR UPDATE`, c.DeliveryID).Scan(&status, &generation, &token, &lease, &count)
	stale := func() (delivery.FinalizationResult, error) {
		slog.InfoContext(ctx, "stale finalization rejected")
		return empty, ErrStaleClaim
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return stale()
	}
	if err != nil {
		return empty, ErrFinalizeUnavailable
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return empty, ErrFinalizeUnavailable
	}
	expectedToken, err := parseCanonicalUUID(c.ClaimToken)
	if err != nil {
		return empty, ErrInvalidInput
	}
	if status != "in_flight" || generation != c.Generation || !token.Valid || token != expectedToken || !lease.Valid || !lease.Time.After(now) {
		return stale()
	}
	decision, err := r.policy.Decide(normalized, count, now)
	if err != nil {
		return empty, err
	}
	var delayMS *int64
	if decision.Status == "pending" {
		value := decision.RetryDelay.Milliseconds()
		delayMS = &value
		next := now.Add(decision.RetryDelay)
		decision.NextAttemptAt = &next
	}
	var errorCode *string
	if normalized.ErrorCode != "" {
		errorCode = &normalized.ErrorCode
	}
	tag, err := tx.Exec(ctx, `UPDATE attempts SET outcome=$5,finished_at=$6,http_status=$7,error_code=$8,duration_ms=$9,retry_delay_ms=$10 WHERE id=$1 AND delivery_id=$2 AND generation=$3 AND claim_token=$4 AND outcome='running'`, c.AttemptID, c.DeliveryID, c.Generation, c.ClaimToken, string(normalized.Outcome), now, normalized.HTTPStatus, errorCode, normalized.Duration.Milliseconds(), delayMS)
	if err != nil {
		return empty, ErrFinalizeUnavailable
	}
	if tag.RowsAffected() != 1 {
		return empty, ErrAttemptInvariant
	}
	var confirmed *time.Time
	if decision.Status == "succeeded" {
		confirmed = &now
	}
	var reason *string
	if decision.TerminalReason != "" {
		reason = &decision.TerminalReason
	}
	tag, err = tx.Exec(ctx, `UPDATE deliveries SET status=$2,lease_until=NULL,claim_token=NULL,confirmed_at=$3,terminal_reason=$4,next_attempt_at=COALESCE($5::timestamptz,next_attempt_at),updated_at=$6 WHERE id=$1`, c.DeliveryID, decision.Status, confirmed, reason, decision.NextAttemptAt, now)
	if err != nil {
		return empty, ErrFinalizeUnavailable
	}
	if tag.RowsAffected() != 1 {
		return empty, ErrAttemptInvariant
	}
	if err = tx.Commit(ctx); err != nil {
		return empty, ErrFinalizeCommitUnknown
	}
	return decision, nil
}
