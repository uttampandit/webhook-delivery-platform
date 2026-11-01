//go:build integration

package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestApplyMigrationsAndRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}

	first, err := database.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := database.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	var heldPID1, heldPID2 int
	if err := first.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&heldPID1); err != nil {
		t.Fatal(err)
	}
	if err := second.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&heldPID2); err != nil {
		t.Fatal(err)
	}
	if heldPID1 == heldPID2 {
		t.Fatalf("held pool connections share backend pid %d", heldPID1)
	}
	bootstrapDir := t.TempDir()
	if err := Apply(ctx, database.Pool, bootstrapDir); err != nil {
		t.Fatal("bootstrap migration metadata:", err)
	}

	if _, err := database.Pool.Exec(ctx, `CREATE TABLE migration_runner_probes (
			version bigint PRIMARY KEY,
			migration_pid integer NOT NULL,
			migration_txid bigint NOT NULL,
			migration_lock_held boolean NOT NULL,
			metadata_pid integer,
			metadata_txid bigint,
			metadata_lock_held boolean
		)`); err != nil {
		t.Fatal("create session observation table:", err)
	}
	if _, err := database.Pool.Exec(ctx, `CREATE FUNCTION capture_migration_session() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			UPDATE migration_runner_probes
			SET metadata_pid = pg_backend_pid(),
				metadata_txid = txid_current(),
				metadata_lock_held = EXISTS (
					SELECT 1 FROM pg_locks
					WHERE locktype = 'advisory'
					  AND pid = pg_backend_pid()
					  AND granted
					  AND classid = ((7340202601 >> 32)::bigint)::oid
					  AND objid = ((7340202601 & 4294967295)::bigint)::oid
					  AND objsubid = 1
				)
			WHERE version = NEW.version;
			RETURN NEW;
		END;
		$$`); err != nil {
		t.Fatal("create session observation function:", err)
	}
	if _, err := database.Pool.Exec(ctx, `CREATE TRIGGER capture_migration_session
		BEFORE INSERT ON schema_migrations
		FOR EACH ROW EXECUTE FUNCTION capture_migration_session();`); err != nil {
		t.Fatal("create session observation trigger:", err)
	}

	fixtureDir := filepath.Join("testdata", "migrations")
	if err := Apply(ctx, database.Pool, fixtureDir); err != nil {
		t.Fatal("first migration run:", err)
	}
	var firstCount int
	if err := database.Pool.QueryRow(ctx, "SELECT count(*) FROM migration_fixture").Scan(&firstCount); err != nil {
		t.Fatal(err)
	}
	if firstCount != 2 {
		t.Fatalf("fixture row count = %d; want 2", firstCount)
	}
	assertMigrationRecords(t, ctx, database, heldPID1, heldPID2)

	before := readMigrationMetadata(t, ctx, database)
	if err := Apply(ctx, database.Pool, fixtureDir); err != nil {
		t.Fatal("repeat migration run:", err)
	}
	after := readMigrationMetadata(t, ctx, database)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("repeat migration run changed metadata: before %v, after %v", before, after)
	}

	changedDir := copyFixtureMigrations(t, fixtureDir)
	changedPath := filepath.Join(changedDir, "001_create_fixture.sql")
	contents, err := os.ReadFile(changedPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(changedPath, append(contents, []byte("\n-- changed checksum\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Apply(ctx, database.Pool, changedDir); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("changed checksum error = %v; want checksum change failure", err)
	}

	failedDir := copyFixtureMigrations(t, fixtureDir)
	writeMigrationFile(t, failedDir, "003_fail.sql", `CREATE TABLE migration_should_rollback (id integer PRIMARY KEY);
SELECT migration_function_that_does_not_exist();
`)
	if err := Apply(ctx, database.Pool, failedDir); err == nil {
		t.Fatal("migration with invalid SQL unexpectedly succeeded")
	}
	var recorded int
	if err := database.Pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations WHERE version = 3").Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded != 0 {
		t.Fatalf("failed migration metadata rows = %d; want 0", recorded)
	}
	var rolledBackTable *string
	if err := database.Pool.QueryRow(ctx, "SELECT to_regclass('migration_should_rollback')::text").Scan(&rolledBackTable); err != nil {
		t.Fatal(err)
	}
	if rolledBackTable != nil {
		t.Fatalf("failed migration left table %q", *rolledBackTable)
	}
}

type migrationMetadata struct {
	Version  int64
	Name     string
	Checksum string
	Applied  time.Time
}

type migrationProcessResult struct {
	index int
	err   error
}

func readMigrationMetadata(t *testing.T, ctx context.Context, database *testutil.Database) []migrationMetadata {
	t.Helper()
	rows, err := database.Pool.Query(ctx, "SELECT version, name, sha256, applied_at FROM schema_migrations ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []migrationMetadata
	for rows.Next() {
		var item migrationMetadata
		if err := rows.Scan(&item.Version, &item.Name, &item.Checksum, &item.Applied); err != nil {
			t.Fatal(err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertMigrationRecords(t *testing.T, ctx context.Context, database *testutil.Database, heldPIDs ...int) {
	t.Helper()
	rows, err := database.Pool.Query(ctx, `
		SELECT version, migration_pid, migration_txid, migration_lock_held,
		       metadata_pid, metadata_txid, metadata_lock_held
		FROM migration_runner_probes ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var version int64
		var migrationPID, metadataPID int
		var migrationTxID, metadataTxID int64
		var migrationLockHeld, metadataLockHeld bool
		if err := rows.Scan(&version, &migrationPID, &migrationTxID, &migrationLockHeld, &metadataPID, &metadataTxID, &metadataLockHeld); err != nil {
			t.Fatal(err)
		}
		count++
		if migrationPID != metadataPID || migrationTxID != metadataTxID {
			t.Errorf("migration %d ran SQL on pid/txid %d/%d and recorded metadata on %d/%d", version, migrationPID, migrationTxID, metadataPID, metadataTxID)
		}
		if !migrationLockHeld || !metadataLockHeld {
			t.Errorf("migration %d did not hold the advisory lock through SQL and metadata insert", version)
		}
		for _, pid := range heldPIDs {
			if migrationPID == pid {
				t.Errorf("migration %d reused one of the separately-held pool sessions (pid %d)", version, pid)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("migration connection probes = %d; want 2", count)
	}
}

func TestMigrationProcessesSerialize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeMigrationFile(t, dir, "001_serialized.sql", "CREATE TABLE serialized_migration_applied (id integer PRIMARY KEY);\n")
	binary := buildMigrationCLI(t)

	holder, err := database.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	locked := false
	defer func() {
		if locked {
			var unlocked bool
			_ = holder.QueryRow(context.Background(), "SELECT pg_advisory_unlock(7340202601)").Scan(&unlocked)
		}
		holder.Release()
	}()
	if _, err := holder.Exec(ctx, "SELECT pg_advisory_lock(7340202601)"); err != nil {
		t.Fatal("hold migration lock:", err)
	}
	locked = true
	var holderHasLock bool
	if err := holder.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_locks
			WHERE locktype = 'advisory' AND granted
			  AND classid = ((7340202601 >> 32)::bigint)::oid
			  AND objid = ((7340202601 & 4294967295)::bigint)::oid
			  AND objsubid = 1
			  AND database = (SELECT oid FROM pg_database WHERE datname = $1)
		)`, database.Name()).Scan(&holderHasLock); err != nil {
		t.Fatal("verify held migration lock:", err)
	}
	if !holderHasLock {
		t.Fatalf("test session did not hold migration lock %d in %s", migrationLockKey, database.Name())
	}
	childConfig, err := pgxpool.ParseConfig(database.AdminConnectionString())
	if err != nil || childConfig.ConnConfig.Database != "postgres" {
		t.Fatalf("child migration admin DSN selected database %q; want postgres (parse error: %v)", childConfigDatabase(childConfig), err)
	}

	commands := make([]*exec.Cmd, 0, 2)
	outputs := make([]*bytes.Buffer, 0, 2)
	processResults := make(chan migrationProcessResult, 2)
	for i := 0; i < 2; i++ {
		commandCtx, commandCancel := context.WithCancel(ctx)
		t.Cleanup(commandCancel)
		cmd := exec.CommandContext(commandCtx, binary, "--dir", dir, "--database", database.Name())
		cmd.Env = replaceEnv(os.Environ(), "DATABASE_URL", database.AdminConnectionString())
		output := &bytes.Buffer{}
		cmd.Stdout = output
		cmd.Stderr = output
		if err := cmd.Start(); err != nil {
			t.Fatal("start migration process:", err)
		}
		commands = append(commands, cmd)
		outputs = append(outputs, output)
		go func(index int, command *exec.Cmd) {
			processResults <- migrationProcessResult{index: index, err: command.Wait()}
		}(i, cmd)
	}

	waitForAdvisoryLockWaiters(t, ctx, database, 2, processResults, outputs)
	var unlocked bool
	if err := holder.QueryRow(ctx, "SELECT pg_advisory_unlock(7340202601)").Scan(&unlocked); err != nil || !unlocked {
		t.Fatalf("release migration lock: unlocked=%t error=%v", unlocked, err)
	}
	locked = false

	for range commands {
		result := <-processResults
		if result.err != nil {
			t.Fatalf("migration process %d failed: %v; output=%q", result.index+1, result.err, safeProcessOutput(outputs[result.index].String()))
		}
	}
	var versions int
	if err := database.Pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations WHERE version = 1").Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 1 {
		t.Fatalf("serialized processes recorded version 1 %d times; want once", versions)
	}
}

func buildMigrationCLI(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate integration test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../.."))
	binary := filepath.Join(t.TempDir(), "migrate")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/migrate")
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build migration CLI: %v (%s)", err, safeProcessOutput(string(output)))
	}
	return binary
}

func waitForAdvisoryLockWaiters(t *testing.T, ctx context.Context, database *testutil.Database, want int, processResults <-chan migrationProcessResult, outputs []*bytes.Buffer) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		err := database.Pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_locks
			WHERE locktype = 'advisory' AND NOT granted
			  AND classid = ((7340202601 >> 32)::bigint)::oid
			  AND objid = ((7340202601 & 4294967295)::bigint)::oid
			  AND objsubid = 1
			  AND database = (SELECT oid FROM pg_database WHERE datname = $1)`, database.Name()).Scan(&count)
		if err != nil {
			t.Fatal("inspect advisory lock waiters:", err)
		}
		if count == want {
			return
		}
		select {
		case result := <-processResults:
			if result.err != nil {
				t.Fatalf("migration process %d exited before waiting for the advisory lock: %v; output=%q", result.index+1, result.err, safeProcessOutput(outputs[result.index].String()))
			}
			t.Fatalf("migration process %d exited before waiting for the advisory lock; output=%q", result.index+1, safeProcessOutput(outputs[result.index].String()))
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %d migration lock waiters; observed %d", want, count)
		case <-ticker.C:
		}
	}
}

func copyFixtureMigrations(t *testing.T, source string) string {
	t.Helper()
	dest := t.TempDir()
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dest, entry.Name()), contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dest
}

func replaceEnv(environment []string, key, value string) []string {
	result := make([]string, 0, len(environment)+1)
	prefix := key + "="
	for _, item := range environment {
		if !strings.HasPrefix(item, prefix) {
			result = append(result, item)
		}
	}
	return append(result, prefix+value)
}

func safeProcessOutput(output string) string {
	if strings.Contains(output, "postgres://") || strings.Contains(output, "password=") {
		return "<redacted output>"
	}
	return strings.TrimSpace(output)
}

func childConfigDatabase(config *pgxpool.Config) string {
	if config == nil || config.ConnConfig == nil {
		return "<unavailable>"
	}
	return config.ConnConfig.Database
}
