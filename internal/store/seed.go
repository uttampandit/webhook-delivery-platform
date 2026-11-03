package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

const seedRollbackTimeout = 5 * time.Second

// Seed inserts the fixed tenant and destination registry in one transaction.
// Existing rows are accepted only when every immutable field still matches;
// this function never updates seeded values to accommodate configuration drift.
func Seed(ctx context.Context, pool *pgxpool.Pool, registry config.Registry) (err error) {
	if pool == nil {
		return errors.New("seed database pool is nil")
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.New("begin seed transaction")
	}
	committed := false
	defer func() {
		if !committed {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), seedRollbackTimeout)
			defer cancel()
			_ = tx.Rollback(rollbackCtx)
		}
	}()

	tenant := registry.Tenant()
	if _, err := tx.Exec(ctx, `
		INSERT INTO tenants (id, name)
		VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING`, tenant.ID, tenant.Name); err != nil {
		return errors.New("insert seeded tenant")
	}
	var actualTenantName string
	if err := tx.QueryRow(ctx, `SELECT name FROM tenants WHERE id = $1 FOR UPDATE`, tenant.ID).Scan(&actualTenantName); err != nil {
		return errors.New("read seeded tenant")
	}
	if actualTenantName != tenant.Name {
		return errors.New("seed drift: tenant name differs from the fixed registry")
	}

	for _, destination := range registry.Destinations() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO destinations (id, tenant_id, code, url, key_ref)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (id) DO NOTHING`,
			destination.ID, destination.TenantID, destination.Code, destination.URL, destination.KeyRef); err != nil {
			return errors.New("insert seeded destination; an existing row conflicts with the fixed registry")
		}

		var actualTenantID, actualCode, actualURL, actualKeyRef string
		if err := tx.QueryRow(ctx, `
			SELECT tenant_id::text, code, url, key_ref
			FROM destinations
			WHERE id = $1
			FOR UPDATE`, destination.ID).Scan(&actualTenantID, &actualCode, &actualURL, &actualKeyRef); err != nil {
			return errors.New("read seeded destination")
		}
		if actualTenantID != destination.TenantID || actualCode != destination.Code ||
			actualURL != destination.URL || actualKeyRef != destination.KeyRef {
			return errors.New("seed drift: destination " + destination.Code + " differs from the fixed registry")
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.New("commit seed transaction")
	}
	committed = true
	return nil
}
