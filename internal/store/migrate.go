package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationLockKey int64 = 7340202601

const migrationLockTimeout = 5 * time.Second

// Apply applies forward-only SQL migrations from dir. The advisory lock,
// metadata queries, and every migration transaction use one dedicated pool
// connection so the session lock covers all changes.
func Apply(ctx context.Context, pool *pgxpool.Pool, dir string) (err error) {
	if pool == nil {
		return errors.New("migration pool is nil")
	}

	migrations, err := readMigrations(dir)
	if err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	lockAttempted := false
	defer func() {
		if lockAttempted {
			err = errors.Join(err, releaseMigrationLock(conn))
		} else {
			conn.Release()
		}
	}()

	lockAttempted = true
	if _, err = conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}

	if _, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version bigint PRIMARY KEY,
			name text NOT NULL,
			sha256 text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`); err != nil {
		return fmt.Errorf("bootstrap schema_migrations: %w", err)
	}

	applied, err := readAppliedMigrations(ctx, conn)
	if err != nil {
		return err
	}

	known := make(map[int64]migration, len(migrations))
	for _, migration := range migrations {
		known[migration.version] = migration
	}
	for version, previous := range applied {
		if _, ok := known[version]; !ok {
			return fmt.Errorf("database has migration %d (%q) but no matching SQL file exists in %s", version, previous.name, dir)
		}
	}

	for _, migration := range migrations {
		if previous, ok := applied[migration.version]; ok {
			if previous.name != migration.name || previous.sha256 != migration.sha256 {
				return fmt.Errorf("migration %d checksum or name changed: database has %q (%s), files have %q (%s)", migration.version, previous.name, previous.sha256, migration.name, migration.sha256)
			}
			continue
		}
		if err := applyMigration(ctx, conn, migration); err != nil {
			return err
		}
	}

	return nil
}

func readAppliedMigrations(ctx context.Context, conn *pgxpool.Conn) (map[int64]migration, error) {
	rows, err := conn.Query(ctx, "SELECT version, name, sha256 FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]migration)
	for rows.Next() {
		var item migration
		if err := rows.Scan(&item.version, &item.name, &item.sha256); err != nil {
			return nil, fmt.Errorf("scan schema_migrations: %w", err)
		}
		applied[item.version] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema_migrations rows: %w", err)
	}
	return applied, nil
}

func applyMigration(ctx context.Context, conn *pgxpool.Conn, migration migration) (err error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %d (%s): %w", migration.version, migration.name, err)
	}
	committed := false
	defer func() {
		if !committed {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), migrationLockTimeout)
			defer cancel()
			err = errors.Join(err, tx.Rollback(rollbackCtx))
		}
	}()

	if _, err := tx.Exec(ctx, migration.sql, pgx.QueryExecModeSimpleProtocol); err != nil {
		return fmt.Errorf("execute migration %d (%s): %w", migration.version, migration.name, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO schema_migrations (version, name, sha256)
		VALUES ($1, $2, $3)`, migration.version, migration.name, migration.sha256); err != nil {
		return fmt.Errorf("record migration %d (%s): %w", migration.version, migration.name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %d (%s): %w", migration.version, migration.name, err)
	}
	committed = true
	return nil
}

func releaseMigrationLock(conn *pgxpool.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), migrationLockTimeout)
	defer cancel()

	var unlocked bool
	err := conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", migrationLockKey).Scan(&unlocked)
	if err == nil && unlocked {
		conn.Release()
		return nil
	}

	// A session with an uncertain lock state must never return to the pool.
	pgConn := conn.Hijack()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), migrationLockTimeout)
	defer closeCancel()
	closeErr := pgConn.Close(closeCtx)
	if err != nil {
		return errors.Join(fmt.Errorf("unlock migration advisory lock; discarded connection: %w", err), closeErr)
	}
	return errors.Join(errors.New("migration advisory lock was not held at release; discarded connection"), closeErr)
}
