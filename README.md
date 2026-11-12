# Webhook Delivery & Replay Platform

A local backend project for accepting events, delivering signed webhooks to independent destinations, recovering abandoned work, and replaying exhausted deliveries.

The backend is implemented through **Task 24**, with a subsequent correctness review and regression fixes. **Tasks 20–22 add a memory-only authenticated dashboard, stable event pagination, per-destination delivery timelines, synthetic event creation, and dead-delivery replay controls**. This is a synthetic, single-operator learning project—not a production multi-tenant service.

## What it does

Imagine a shop confirms an order. Fulfillment needs a packing record, while notification needs a notification record. The API accepts one event, stores a delivery for each requested destination, and lets workers deliver them independently. Demo receivers write synthetic database effects; no real messages, shipments, or payments occur.

```text
Publisher → Go API → PostgreSQL ← Go worker → Fulfillment receiver
                                           ↘ Notification receiver
```

The central question is: **what if a receiver commits its work, but the sender never receives its response?** Retrying may repeat an effect. Stable operation IDs and receiver-side transactional deduplication address that risk; sender retries alone cannot guarantee exactly-once effects.

## Implemented behavior

- **Atomic acceptance:** event and requested delivery rows commit together before HTTP 202.
- **Ingest idempotency:** the same tenant/key and exact request bytes return the original event. Changed bytes—including whitespace—conflict. Keys are retained indefinitely.
- **Independent delivery:** PostgreSQL row locks, leases, generations, and tokens coordinate workers. A running attempt is persisted before HTTP, which runs outside the claim transaction.
- **Signed requests:** HMAC-SHA256 binds timestamp, stable operation UUID, and exact persisted wire bytes. Receivers enforce a ±300-second timestamp window.
- **Bounded retries:** uncertainty, HTTP 429 and 5xx retry within five claims per cycle. Other non-2xx statuses are permanent. Jitter and bounded `Retry-After` schedule future work without transaction-held sleeps.
- **Lease recovery:** a worker-side reaper preserves abandoned attempts as unconfirmed and schedules a retry or exhaustion. Fencing rejects stale database writes; it cannot prevent an already-issued external request.
- **Audited replay:** authenticated, idempotent replay starts a fresh cycle for a dead delivery while preserving its operation ID, body, destination snapshots, and lifetime attempt history.
- **Synthetic receivers:** signed normal-mode requests support persisted deduplication on or off. Controls use a separate token-protected listener. Scenarios support `normal`, `http_503`, `http_429` and `commit_drop_once`; the latter consumes a persisted flag and closes the connection after commit.
- **Read API:** authenticated event list/detail expose delivery, attempt and replay history. List pagination uses timestamp/UUID cursors.
- **Bounded observability:** JSON logs retain allowlisted fields and fixed error codes; private Prometheus endpoints expose bounded ingest/attempt counters, declared histograms, and tenant-scoped queue gauges. Runtime counters are not the durable audit record; see [observability details](docs/observability.md).

Defaults: worker concurrency 4, HTTP deadline 5 seconds, lease 30 seconds, lease recovery tick 1 second. Destinations are fixed. API and control tokens come from local configuration.

## Current limits

Real PostgreSQL and HTTP/process tests cover concurrency, rollback, signing, independent fan-out, graceful SIGTERM, lease recovery, replay races, response loss, and actual worker SIGKILL recovery. `make test-crash` kills child workers after claim and after receiver commit, then verifies lease recovery, attempt history, receiver deduplication, and stale-worker fencing. Release binaries ignore hook environment variables. Real HTTP/1.1 tests verify that a socket close after committed receiver effects remains uncertain to the sender; transport read errors and timeouts do not prove that a receiver applied no effect.

Recover an outage scenario using authenticated `PATCH /control/scenarios/{id}` with `{"mode":"normal"}`. Dedupe cannot change; a consumed drop cannot be rearmed. HTTP 503/429 modes record rejection before effects and return `Retry-After: 2`.

The dashboard provides token entry/logout, a local API connection status, configured destination codes, cursor-paginated events, event detail, synthetic `order.confirmed` creation, and replay for dead deliveries. Create retries preserve the serialized request bytes and idempotency key after an uncertain response; editing a field starts a new request identity. Replay status says when work is scheduled, not delivered, and a 409 reloads current delivery state. Payloads render as escaped text and uncertainty remains visible. The token exists only in page memory, so a refresh requires re-entry. Creating a scenario still uses `democtl`; the browser does not change receiver controls or accept arbitrary destination URLs. The full local Compose runtime serves the built UI and keeps receiver/control/metrics listeners on the internal network. Task 25 choreography, benchmarks, and the final release audit remain incomplete. There are no measured throughput, adoption or production-readiness claims.

## Stack and prerequisites

Go standard-library HTTP and pgx, PostgreSQL 17, React/TypeScript with Vite, and Docker Compose. No broker, ORM, paid API or cloud dependency.

Use the repository's pinned tools: Go **1.27.3** (module language 1.27), Node **26.13.0**, npm **11.20.0**, and PostgreSQL **17.11** via Compose. Node/npm are needed for frontend scaffold checks. `make doctor` checks the local toolchain.

## Run locally

Start Docker, then initialize private configuration once and start the full Compose runtime:

```sh
make init-env
make up
```

If `.env` already exists, keep it and skip `make init-env`; the helper refuses to overwrite it. Compose builds the application image, waits for PostgreSQL health, applies migrations, seeds fixed destinations, and then waits for the API, worker, and both synthetic receivers. Open [http://127.0.0.1:8080](http://127.0.0.1:8080) and enter the `API_TOKEN` from `.env`; the dashboard keeps it in page memory only. Never commit `.env` or paste credentials into commands, URLs, or reports.

Compose seeds a separate `webhook_compose_dev` database so it does not overwrite destination URLs already stored in the older `webhook_dev` host-mode database. Both databases live in the same named PostgreSQL volume. To stop the normal local runtime while keeping data, run:

```sh
make down
```

`make down` removes containers and the network but retains the named volume. `docker compose down -v` deletes that volume and its databases; use it only when you intend to discard local data.

## Internal demo commands

`democtl` is included in the API image and receives credentials through the container environment. Create a fresh scenario, publish an event, and inspect receiver effects with:

```sh
docker compose exec -T api /app/democtl scenario create --id walkthrough --receiver both --mode normal --dedupe on
# Copy the returned fresh scenario_id into the next commands.
docker compose exec -T api /app/democtl publish --scenario <scenario_id> --order order_1001 --destinations fulfillment,notification
docker compose exec -T api /app/democtl wait --event <event_id> --state succeeded --timeout 90s
docker compose exec -T api /app/democtl scenario inspect --id <scenario_id> --receiver both
docker compose exec -T api /app/democtl scenario recover --id <scenario_id> --receiver fulfillment
docker compose exec -T api /app/democtl replay --delivery <dead_delivery_id> --key replay_request_1
```

Creation appends a random suffix, so repeating the base name creates a fresh scenario without deleting data. Repeat the same publish flags for a retry: deterministic bytes produce the same idempotency key. For an exact saved request, use `publish --file request.json --key request_1`; the file's bytes are preserved. Change the scenario/order or explicit key for a new event. Replay always requires an explicit nonsecret request key. Wait checks every delivery and exits nonzero with its last validated statuses on timeout. Full `run` choreography and saved run reports arrive in Task 25.

## Verification

```sh
make doctor
cd web && npm ci && cd ..
make build
make up
make compose-smoke
make down
make up
make compose-smoke
make web-check
make test
make test-race
make vet

# With Docker/PostgreSQL running and .env loaded:
make test-integration
```

Integration tests require `TEST_DATABASE_ADMIN_URL` pointing to the PostgreSQL `postgres` maintenance database with database creation/drop privileges. The guarded harness creates and removes isolated `webhook_test_<hex>` databases; it does not run application tests against the development database. No SQLite substitute is used for PostgreSQL locking behavior.

An independent public HMAC fixture is reproducible with:

```sh
python3 internal/signing/testdata/vector.py
```

## Learn the design

- [Project explained in everyday language](learning/PROJECT_EXPLAINED.md)
- [Walkthrough by implementation milestone](learning/WALKTHROUGH.md)
- [Prediction questions and safe experiments](learning/EXERCISES.md)

The guides distinguish implemented behavior from later experiments. A useful starting question: an event has two deliveries and one fails—why should the healthy receiver's effect remain independent?

This repository began as a portable, ordered implementation kit. The local kit also contains task cards, specs, resume/teaching prompts and progress/evidence files for ongoing sessions; these private working documents are not part of the published README bundle. The overview and learning guides are public, while personal notes, agent instructions and scheduling state stay local. Passing tests provides implementation evidence, not proof of a learner's understanding.
