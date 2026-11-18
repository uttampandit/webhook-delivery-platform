# Implementation overview and design decisions

The platform accepts events into PostgreSQL and delivers signed webhooks independently to two fixed destinations. An operator dashboard exposes delivery state, attempt history, and replay controls.

The central failure boundary is a receiver committing an effect before the sender records acknowledgment. Real worker-crash tests reproduce this window: recovery can issue a second request with the same operation ID, while receiver-side transactional deduplication prevents a second effect. Sender fencing protects database writes; it cannot undo an issued HTTP request.

Three bounded 200-event benchmark runs reconcile events, attempts, and receiver effects. They describe a finite local workload. The current deployment uses one operator, fixed destinations, and a single Compose host; see [limitations](limitations.md) for operational boundaries.

## Architecture decisions

1. **Acceptance:** `POST /v1/events` authenticates and validates; `EventRepository.AcceptEvent` inserts the event and its requested deliveries in one PostgreSQL transaction. The same tenant/key and same raw bytes return the existing event; different bytes conflict. See [handler](../internal/api/events_create.go#L20) and [repository](../internal/store/events.go#L158).
2. **Independent delivery:** `ClaimDue` locks only ready delivery rows with `SKIP LOCKED`, inserts the `running` attempt, and commits the lease/token/generation before loading immutable send data. Each destination has its own delivery state. See [claims](../internal/store/claims.go#L35).
3. **External effect:** `HTTPSender` signs the exact persisted wire body and calls the fixed destination after the claim transaction ends. The receiver transaction records requests, dedupe keys, and effects. See [sender](../internal/delivery/http.go#L33) and [receiver](../internal/store/demo.go#L122).
4. **Uncertainty and fencing:** after response loss or process death, the reaper preserves an unconfirmed attempt and schedules recovery. A replacement changes the generation and token; a stale worker cannot finalize over the replacement. Already-issued HTTP can still repeat. See [recovery](../internal/store/recovery.go#L38), [finalization](../internal/store/finalize.go#L33), and [crash tests](../tests/crash/crash_test.go#L347).
5. **Replay and operator view:** the dashboard shows per-destination attempt/replay history and allows explicit replay of a dead delivery. Replay opens another cycle, keeps the operation ID, and reports “scheduled,” not “delivered.” See [replay store](../internal/store/replay.go#L27) and [demo evidence](demo.md).
6. **Trade-off and limit:** PostgreSQL keeps durable state and removes a separate broker from this demo, but this project is not a highly available scheduler. The shared local database and receiver scenario rows also influence the finite benchmark. See [architecture](architecture.md), [limitations](limitations.md), and [measured results](benchmark.md).

## Delivery terminology

- **Lease:** temporary permission for a worker to own a delivery.
- **Fence:** generation/token checks that reject stale database writes; not a network cancellation mechanism.
- **Operation ID:** stable identity for one destination delivery across attempts and replay.
- **Unconfirmed:** no usable acknowledgment reached the sender; the remote effect may already have happened.
- **At least once:** retries may send the same logical delivery more than once. Receiver idempotency, not sender fencing, suppresses repeated effects.

## Failure analysis questions

- What exactly proves the event and both delivery rows committed before `202`?
- What survives a `SIGKILL` after a claim but before HTTP? What survives a kill after the receiver effect?
- Why does the stale finalizer fail, and why can an already-issued request still reach the receiver?
- What did the benchmark count as an event, a delivery, an attempt, a request, and an effect? Why is that run not maximum capacity?
- What would need to change before exposing arbitrary destinations or multiple customers?

See [failure scenarios and experiments](../learning/EXERCISES.md) for reproducible ways to explore these boundaries.

## Development acknowledgment

AI coding tools assisted with the implementation and documentation.
