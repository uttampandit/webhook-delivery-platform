# Local operations guide

This guide operates the synthetic Compose project on one machine. It does not deploy or expose a production service.

## Start from a clean shell

Requirements: Docker with Compose v2, Go 1.27.3, Node 26.13.0, and npm 11.20.0. Start Docker Desktop first. Keep `.env` private and skip initialization if it already exists.

```sh
make doctor
make init-env  # only when .env does not exist
make up
curl -fsS http://127.0.0.1:8080/health/ready
```

`make up` builds and starts the local stack, waits for health, applies migrations, and seeds fixed destinations. The Compose database is `webhook_compose_dev`, separate from the older host-mode database. The ready endpoint returns only a generic ready/not-ready status. Do not publish `.env`, tokens, connection strings, or screenshots containing them.

Open [http://127.0.0.1:8080](http://127.0.0.1:8080), enter the API token privately, and inspect events. The dashboard keeps the token in browser memory; refreshing requires re-entry. It does not configure arbitrary URLs or receiver control scenarios.

## Walk a failure and recovery

```sh
make demo
```

This creates five new synthetic scenarios and events, waits for delivery state, checks receiver requests/effects, and saves a sanitized report under `artifacts/demo/<run-id>/report.json`. Repeated runs leave database history in place. The tested scenarios and counts are documented in the [demo walkthrough](demo.md).

For a worker process-death test against isolated PostgreSQL test databases:

```sh
make test-crash
```

It requires `TEST_DATABASE_ADMIN_URL` to point at the PostgreSQL `postgres` maintenance database with database create/drop privileges. The harness creates and removes `webhook_test_<hex>` databases. Never point it at a development or production database; it is not a substitute for testing real PostgreSQL locking.

## Inspect and diagnose

```sh
docker compose ps
docker compose logs --tail=100 api worker fulfillment notification postgres
```

- If a service is stopped or unhealthy, inspect its Compose health/logs and rerun `make up`; it waits for health and does not remove volumes.
- `/health/live` checks process liveness; `/health/ready` checks the API's PostgreSQL/schema/destination readiness.
- An `unconfirmed` attempt means the sender has no usable acknowledgment. The receiver may have committed an effect. Check the event's per-destination attempt timeline and receiver scenario ledger before deciding to replay.
- The worker's reaper scans every second for expired leases. Recovery preserves the consumed attempt, advances its fence, and retries or exhausts it under the bounded policy. A stale worker can still have an already-issued HTTP request in flight.
- A dead delivery can be replayed through the dashboard. Replay records the operator action, opens another cycle, and preserves the delivery operation ID. The status indicates work was scheduled, not that the receiver applied it; replay may cause another request.
- JSON application logs use an allowlist and bounded error codes. Private metrics are not host-published. Runtime counters reset with the process; PostgreSQL history is the durable audit source.

## Tests, benchmark, and stop

```sh
make test
make vet
```

PostgreSQL integration tests require the guarded admin URL described above. The test harness creates isolated databases and drops those test databases; it must not be aimed at `webhook_dev` or `webhook_compose_dev`.

The optional `make benchmark` command requires all default Compose services to already be healthy. It runs a bounded one-off container on the internal network and adds events/receiver history; use a fresh `BENCHMARK_RUN_ID` each time. It is a finite local workload, not a load test for maximum sustainable throughput. See [the limits and exact measured runs](benchmark.md).

```sh
make down
```

`make down` stops and removes the normal Compose containers/network while retaining the named PostgreSQL volume. No automatic backup, restore, or disaster-recovery process is included. Avoid `docker compose down -v` unless you intentionally want to delete local database history.
