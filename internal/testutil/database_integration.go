//go:build integration

package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maintenanceDatabase = "postgres"

var generatedDatabaseName = regexp.MustCompile(`^webhook_test_[0-9a-f]{32}$`)

// Database is a freshly-created PostgreSQL database owned by one integration test.
type Database struct {
	Pool      *pgxpool.Pool
	name      string
	adminPool *pgxpool.Pool
	targetCfg *pgxpool.Config
	adminDSN  string
}

// NewDatabase creates one isolated test database and registers guarded cleanup
// with t. It never accepts a database URL from a test-specific fallback.
func NewDatabase(ctx context.Context, t testing.TB) (*Database, error) {
	t.Helper()
	adminURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_ADMIN_URL"))
	if adminURL == "" {
		return nil, errors.New("TEST_DATABASE_ADMIN_URL is required; point it to PostgreSQL's postgres maintenance database")
	}

	adminCfg, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		return nil, errors.New("TEST_DATABASE_ADMIN_URL is invalid")
	}
	adminCfg.MaxConns = 4
	adminPool, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		return nil, errors.New("cannot configure the PostgreSQL test administrator connection")
	}

	var currentDatabase string
	if err := adminPool.QueryRow(ctx, "SELECT current_database()").Scan(&currentDatabase); err != nil {
		adminPool.Close()
		return nil, errors.New("cannot connect using TEST_DATABASE_ADMIN_URL; verify PostgreSQL is running and the credentials are valid")
	}
	if currentDatabase != maintenanceDatabase {
		adminPool.Close()
		return nil, fmt.Errorf("TEST_DATABASE_ADMIN_URL must connect to the %q maintenance database (connected to %q)", maintenanceDatabase, currentDatabase)
	}

	var randomName [16]byte
	if _, err := rand.Read(randomName[:]); err != nil {
		adminPool.Close()
		return nil, errors.New("cannot generate an isolated PostgreSQL test database name")
	}
	name := "webhook_test_" + hex.EncodeToString(randomName[:])
	if !generatedDatabaseName.MatchString(name) {
		adminPool.Close()
		return nil, errors.New("generated PostgreSQL test database name failed its safety check")
	}
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+(pgx.Identifier{name}).Sanitize()); err != nil {
		adminPool.Close()
		return nil, errors.New("cannot create an isolated test database; verify the test role has CREATEDB privilege")
	}

	targetCfg := adminCfg.Copy()
	targetCfg.ConnConfig.Database = name
	targetCfg.MaxConns = 4
	database := &Database{name: name, adminPool: adminPool, targetCfg: targetCfg, adminDSN: adminCfg.ConnString()}
	t.Cleanup(func() { database.cleanup(t) })

	database.Pool, err = pgxpool.NewWithConfig(ctx, targetCfg.Copy())
	if err != nil {
		return nil, errors.New("cannot configure the isolated PostgreSQL test database connection")
	}
	if err := database.Pool.Ping(ctx); err != nil {
		return nil, errors.New("cannot connect to the newly-created isolated test database")
	}
	if err := requireCurrentDatabase(ctx, database.Pool, name); err != nil {
		return nil, err
	}
	return database, nil
}

// Name returns the generated database name for assertions that verify isolation.
func (database *Database) Name() string {
	return database.name
}

// AdminConnectionString is intended for a child migration process whose target
// database is selected separately with the migration CLI's --database option.
func (database *Database) AdminConnectionString() string {
	return database.adminDSN
}

func (database *Database) cleanup(t testing.TB) {
	t.Helper()
	if database.Pool != nil {
		database.Pool.Close()
	}
	if !generatedDatabaseName.MatchString(database.name) {
		t.Errorf("refusing to drop test database with unexpected generated name %q", database.name)
		database.adminPool.Close()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	probePool, err := pgxpool.NewWithConfig(ctx, database.targetCfg.Copy())
	if err != nil {
		t.Error("cannot verify isolated database before cleanup")
		database.adminPool.Close()
		return
	}
	if err := requireCurrentDatabase(ctx, probePool, database.name); err != nil {
		t.Errorf("refusing to drop isolated database: %v", err)
		probePool.Close()
		database.adminPool.Close()
		return
	}
	probePool.Close()

	var currentDatabase string
	if err := database.adminPool.QueryRow(ctx, "SELECT current_database()").Scan(&currentDatabase); err != nil || currentDatabase != maintenanceDatabase {
		if err != nil {
			t.Error("cannot verify maintenance database before cleanup")
		} else {
			t.Errorf("refusing to drop test database from unexpected maintenance database %q", currentDatabase)
		}
		database.adminPool.Close()
		return
	}
	if _, err := database.adminPool.Exec(ctx, "DROP DATABASE "+(pgx.Identifier{database.name}).Sanitize()); err != nil {
		t.Errorf("cannot drop isolated database %q; verify the test role has DROP privilege", database.name)
	}
	database.adminPool.Close()
}

func requireCurrentDatabase(ctx context.Context, pool *pgxpool.Pool, expected string) error {
	var currentDatabase string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&currentDatabase); err != nil {
		return errors.New("cannot verify PostgreSQL current_database()")
	}
	if currentDatabase != expected {
		return fmt.Errorf("current_database() is %q; expected generated database %q", currentDatabase, expected)
	}
	return nil
}
