# Webhook Delivery & Replay Platform

A local backend project for accepting events, delivering signed webhooks to independent destinations, recovering abandoned work, and replaying exhausted deliveries.

The backend is implemented through **Task 18**, with a subsequent correctness review and regression fixes. A React dashboard and the full release workflow are still planned. This is a synthetic, single-operator learning project—not a production multi-tenant service.

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

Defaults: worker concurrency 4, HTTP deadline 5 seconds, lease 30 seconds, lease recovery tick 1 second. Destinations are fixed. API and control tokens come from local configuration.

## Current limits

Real PostgreSQL and HTTP/process tests cover concurrency, rollback, signing, independent fan-out, graceful SIGTERM, lease recovery, replay races and response loss. **Actual SIGKILL recovery remains a later milestone.** Real HTTP/1.1 tests now verify a socket close after committed receiver effects, deduplication on/off, and consumed flags surviving process restart. Transport read errors and timeouts do not prove that a receiver applied no effect.

Recover an outage scenario using authenticated `PATCH /control/scenarios/{id}` with `{"mode":"normal"}`. Dedupe cannot change; a consumed drop cannot be rearmed. HTTP 503/429 modes record rejection before effects and return `Retry-After: 2`.

The frontend is a scaffold. Docker Compose currently runs PostgreSQL; the API, worker and two receivers run separately on the host. Full Compose runtime, dashboard, full scripted demo, metrics, benchmarks and the final release audit are not complete. There are no measured throughput, adoption or production-readiness claims.

## Stack and prerequisites

Go standard-library HTTP and pgx, PostgreSQL 17, React/TypeScript with Vite, and Docker Compose. No broker, ORM, paid API or cloud dependency.

Use the repository's pinned tools: Go **1.27.3** (module language 1.27), Node **26.13.0**, npm **11.20.0**, and PostgreSQL **17.11** via Compose. Node/npm are needed for frontend scaffold checks. `make doctor` checks the local toolchain.

## Run locally

Start Docker first. From the repository root, initialize private configuration once:

```sh
make init-env
make up
set -a; . ./.env; set +a
export RUN_MODE=host

go run ./cmd/migrate --dir migrations
go run ./cmd/seed
make build
```

If `.env` already exists, keep it and skip `make init-env`; the helper refuses to overwrite it. It generates random secrets locally. Never commit `.env` or substitute public test-fixture keys for runtime keys.

The commands above seed the **host-mode** destination registry. Existing destination URLs/key references are immutable: if an existing database was seeded in another mode, seeding detects drift rather than overwriting it. Keep the original mode or use a separate development instance.

Open four terminals. In **each**, first load configuration:

```sh
set -a; . ./.env; set +a
```

Then start one process per terminal:

```sh
# Terminal 1
RUN_MODE=host ./bin/api

# Terminal 2
RUN_MODE=host RECEIVER_CODE=fulfillment ./bin/receiver

# Terminal 3
RUN_MODE=host RECEIVER_CODE=notification ./bin/receiver

# Terminal 4
RUN_MODE=host ./bin/worker
```

| Process | Host address |
|---|---|
| API | `127.0.0.1:8080` |
| Fulfillment webhook / controls | `127.0.0.1:8082` / `127.0.0.1:9092` |
| Notification webhook / controls | `127.0.0.1:8083` / `127.0.0.1:9093` |

The API exposes `/health/live` and `/health/ready`. Receiver controls require `DEMO_CONTROL_TOKEN`, including control readiness; they are not served on webhook ports.

Backend contract tests and their limits are mapped in [the verification matrix](docs/verification-matrix.md). Process-crash recovery remains pending Task 19.

## Internal demo commands

After loading `.env` and starting the four host processes above, use `RUN_MODE=host ./bin/democtl`. Credentials come from the environment; do not put them in flags or recordings.

```sh
RUN_MODE=host ./bin/democtl scenario create --id walkthrough --receiver both --mode normal --dedupe on
# Copy the returned fresh scenario_id into the next commands.
RUN_MODE=host ./bin/democtl publish --scenario <scenario_id> --order order_1001 --destinations fulfillment,notification
RUN_MODE=host ./bin/democtl wait --event <event_id> --state succeeded --timeout 90s
RUN_MODE=host ./bin/democtl scenario inspect --id <scenario_id> --receiver both
RUN_MODE=host ./bin/democtl scenario recover --id <scenario_id> --receiver fulfillment
RUN_MODE=host ./bin/democtl replay --delivery <dead_delivery_id> --key replay_request_1
```

Creation appends a random suffix, so repeating the base name creates a fresh scenario without deleting data. Repeat the same publish flags for a retry: deterministic bytes produce the same idempotency key. For an exact saved request, use `publish --file request.json --key request_1`; the file's bytes are preserved. Change the scenario/order or explicit key for a new event. Replay always requires an explicit nonsecret request key. Wait checks every delivery and exits nonzero with its last validated statuses on timeout. Full `run` choreography and saved run reports arrive in Task 25.

## Send an order event

In another terminal, load `.env` and create a normal scenario on both receivers:

```sh
set -a; . ./.env; set +a

for port in 9092 9093; do
  curl -i -X POST "http://127.0.0.1:$port/control/scenarios" \
    -H "Authorization: Bearer $DEMO_CONTROL_TOKEN" \
    -H "Content-Type: application/json" \
    --data '{"scenario_id":"walkthrough_1","mode":"normal","dedupe_enabled":true}'
done
```

Create the event with both fixed destinations:

```sh
curl -i -X POST http://127.0.0.1:8080/v1/events \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Idempotency-Key: walkthrough-order-1001" \
  -H "Content-Type: application/json" \
  --data '{"type":"order.confirmed","payload":{"scenario_id":"walkthrough_1","order_id":"order_1001","quantity":1},"destination_ids":["00000000-0000-4000-8000-000000000101","00000000-0000-4000-8000-000000000102"]}'
```

HTTP 202 means durable acceptance, not completed delivery. Resending the exact body/key returns the original IDs with HTTP 200 and `deduplicated:true`. Changing the bytes under that key returns 409. Use fresh scenario IDs and ingest keys for separate experiments; no reset/delete endpoint is provided.

Inspect events:

```sh
curl -i 'http://127.0.0.1:8080/v1/events?limit=20' \
  -H "Authorization: Bearer $API_TOKEN"

curl -i http://127.0.0.1:9092/control/scenarios/walkthrough_1 \
  -H "Authorization: Bearer $DEMO_CONTROL_TOKEN"
```

| API route | Purpose |
|---|---|
| `GET /v1/destinations` | Fixed destination IDs/codes |
| `GET /v1/events?limit=20&cursor=…` | Compact event summaries with cursor pagination |
| `GET /v1/events/{event_id}` | Payload, delivery states, attempts and replay audit |
| `POST /v1/deliveries/{delivery_id}/replay` | Replay a dead delivery using `Idempotency-Key` and body `{}` |

All `/v1` routes require the API bearer token. A new replay returns 202; a duplicate key/target returns 200 with its original replay identity/cycle. Replay of a pending, in-flight or succeeded delivery returns 409. Replay can repeat external effects, so receiver deduplication still matters.

Stop host processes with Ctrl+C, then stop PostgreSQL:

```sh
make down
```

This removes the container/network and retains the named data volume.

## Verification

```sh
make doctor
make build
make test
make test-race
make vet

# With Docker/PostgreSQL running and .env loaded:
make test-integration

# Frontend scaffold only:
cd web && npm ci && cd ..
make web-check
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
