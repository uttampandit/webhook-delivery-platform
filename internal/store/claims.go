package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/delivery"
)

const MaxClaimSlots = 32 // Same bound as configured worker concurrency.
var (
	ErrClaimUnavailable   = errors.New("claim store unavailable")
	ErrClaimCommitUnknown = errors.New("claim commit outcome is unknown")
)

type ClaimRepository struct {
	pool           *pgxpool.Pool
	lease, timeout time.Duration
}

func NewClaimRepository(pool *pgxpool.Pool, lease, timeout time.Duration) (*ClaimRepository, error) {
	if pool == nil || lease < time.Microsecond || timeout <= 0 {
		return nil, ErrInvalidInput
	}
	return &ClaimRepository{pool, lease, timeout}, nil
}

// ClaimDue consumes only currently free slots. On any error it returns no send
// data, including commit uncertainty or failure loading data after commitment.
// Such leases, if committed, are left for recovery rather than risking a send
// from an unconfirmed claim. No HTTP work occurs in this repository.
func (r *ClaimRepository) ClaimDue(ctx context.Context, freeSlots int) ([]delivery.ClaimedDelivery, error) {
	if freeSlots < 0 || freeSlots > MaxClaimSlots {
		return nil, ErrInvalidInput
	}
	claims := make([]delivery.ClaimedDelivery, 0, freeSlots)
	if freeSlots == 0 {
		return claims, nil
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, ErrClaimUnavailable
	}
	defer tx.Rollback(ctx)
	// Lock only deliveries, never the event rows used later for send data.
	rows, err := tx.Query(ctx, `SELECT id::text FROM deliveries WHERE status='pending' AND next_attempt_at<=clock_timestamp() AND cycle_attempt_count<max_attempts ORDER BY next_attempt_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, freeSlots)
	if err != nil {
		return nil, ErrClaimUnavailable
	}
	ids := make([]string, 0, freeSlots)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, ErrClaimUnavailable
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, ErrClaimUnavailable
	}
	if len(ids) == 0 {
		if err = tx.Commit(ctx); err != nil {
			return nil, ErrClaimCommitUnknown
		}
		return claims, nil
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, ErrClaimUnavailable
	}
	for _, id := range ids {
		token, _, err := newEventUUID()
		if err != nil {
			return nil, ErrClaimUnavailable
		}
		attemptID, _, err := newEventUUID()
		if err != nil {
			return nil, ErrClaimUnavailable
		}
		c := delivery.ClaimedDelivery{DeliveryID: id, AttemptID: attemptID, ClaimToken: token, StartedAt: now}
		err = tx.QueryRow(ctx, `UPDATE deliveries SET status='in_flight',generation=generation+1,claim_token=$2,lease_until=$3::timestamptz+($4::bigint * interval '1 microsecond'),cycle_attempt_count=cycle_attempt_count+1,lifetime_attempt_count=lifetime_attempt_count+1,updated_at=$3 WHERE id=$1 RETURNING event_id::text,cycle_no,cycle_attempt_count,lifetime_attempt_count,generation,lease_until`, id, token, now, r.lease.Microseconds()).Scan(&c.EventID, &c.CycleNo, &c.CycleAttemptNo, &c.AttemptNo, &c.Generation, &c.LeaseUntil)
		if err != nil {
			return nil, ErrClaimUnavailable
		}
		tag, err := tx.Exec(ctx, `INSERT INTO attempts(id,delivery_id,attempt_no,cycle_no,cycle_attempt_no,generation,claim_token,outcome,started_at) VALUES($1,$2,$3,$4,$5,$6,$7,'running',$8)`, c.AttemptID, c.DeliveryID, c.AttemptNo, c.CycleNo, c.CycleAttemptNo, c.Generation, c.ClaimToken, now)
		if err != nil || tag.RowsAffected() != 1 {
			return nil, ErrClaimUnavailable
		}
		claims = append(claims, c)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, ErrClaimCommitUnknown
	}
	// Immutable body and snapshots are loaded only after releasing row locks.
	for i := range claims {
		c := &claims[i]
		err = r.pool.QueryRow(ctx, `SELECT d.url_snapshot,d.key_ref_snapshot,e.wire_body FROM deliveries d JOIN events e ON e.tenant_id=d.tenant_id AND e.id=d.event_id WHERE d.id=$1`, c.DeliveryID).Scan(&c.URLSnapshot, &c.KeyRefSnapshot, &c.WireBody)
		if err != nil {
			return nil, ErrClaimUnavailable
		}
		c.WireBody = append([]byte(nil), c.WireBody...)
	}
	return claims, nil
}
