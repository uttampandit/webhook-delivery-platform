CREATE TABLE migration_fixture (
    name text PRIMARY KEY
);

INSERT INTO migration_fixture (name) VALUES ('first');
INSERT INTO migration_runner_probes (version, migration_pid, migration_txid, migration_lock_held)
SELECT 1, pg_backend_pid(), txid_current(), EXISTS (
    SELECT 1 FROM pg_locks
    WHERE locktype = 'advisory'
      AND pid = pg_backend_pid()
      AND granted
      AND classid = ((7340202601 >> 32)::bigint)::oid
      AND objid = ((7340202601 & 4294967295)::bigint)::oid
      AND objsubid = 1
);
