# Architecture and delivery guarantees

This is a local learning implementation of a webhook sender, operator dashboard, and two synthetic receivers. The configured destinations are fixed; the project is not a customer-facing multi-tenant service.

## Components

```text
publisher
   │ authenticated POST /v1/events
   ▼
Go API ── atomic PostgreSQL transaction ──► events + one delivery per destination
                                                │
                                      PostgreSQL due-work queue
                                                │
                                                ▼
                                         Go worker/reaper
                                      claim + attempt commit
                                                │ signed HTTP outside transaction
                               ┌────────────────┴────────────────┐
                               ▼                                 ▼
                       fulfillment receiver             notification receiver
                       request/dedupe/effect              request/dedupe/effect
                               └────────────────┬────────────────┘
                                                ▼
                                  worker finalizes in PostgreSQL

operator ──► local dashboard/API ──► inspect attempts and replay dead deliveries
```

The PostgreSQL tables hold accepted events, delivery state, attempt history, and replay audit records. PostgreSQL is the scheduler; the implementation does not publish work to a separate broker. Docker Compose is the local runtime and exposes the dashboard/API on loopback. Receiver controls, metrics, and PostgreSQL are not host-published; see [compose.yaml](../compose.yaml).

## One event, two independent deliveries

1. `POST /v1/events` is authenticated, size-limited, validates the idempotency key and request, then calls `EventRepository.AcceptEvent` ([API handler](../internal/api/events_create.go#L20), [transaction](../internal/store/events.go#L158)). In one PostgreSQL transaction, the repository inserts the event and all requested delivery rows. It returns only after commit. The same tenant/key and byte-identical request returns the original event; different bytes conflict.
2. The worker claims due delivery rows using short transactions and `FOR UPDATE SKIP LOCKED`. For each claim it increments the generation and lifetime/cycle counters, stores a lease token, and inserts a `running` attempt before committing ([claims](../internal/store/claims.go#L35)). It loads immutable body and destination snapshots only after row locks are released.
3. `HTTPSender.Send` signs the persisted wire body and sends one bounded HTTP request outside the database transaction ([sender](../internal/delivery/http.go#L33)). Fulfillment and notification have separate delivery rows, so one can fail while the other succeeds.
4. The worker finalizes the attempt and delivery in a separate transaction. The finalizer accepts a write only when delivery status, generation, claim token, and unexpired lease still match ([worker](../internal/delivery/worker.go#L75), [finalizer](../internal/store/finalize.go#L33)).

## Lease recovery, fencing, and the response-loss window

A **lease** is a time-bounded claim. If a process stops after claiming, the reaper marks the still-running attempt `unconfirmed` with `lease_expired`, advances the generation, clears the old token, and schedules retry or marks the delivery dead according to policy ([recovery](../internal/store/recovery.go#L38), [reaper loop](../internal/delivery/reaper.go#L21)). **Fencing** means an old worker cannot update database state after a replacement claim changes the generation/token or the lease expires. It cannot revoke an HTTP request already issued.

The receiver can commit an effect and the worker can then die before finalizing success. After lease recovery, the sender may issue the same stable delivery operation again. The synthetic receiver's own transaction records the request, checks the operation ID in its dedupe ledger, and inserts an effect only once ([receiver transaction](../internal/store/demo.go#L122)). This demonstrates duplicate-request suppression by the receiver. It does not promise exactly-once external effects for arbitrary destinations.

The real child-process test `TestCrashAfterReceiverEffectRepeatsRequestButKeepsOneEffect` kills a worker after its HTTP call and before finalization, then verifies two receiver requests and one effect after recovery ([crash test](../tests/crash/crash_test.go#L347)). The adjacent stale-worker test verifies the old process cannot overwrite the replacement's successful state ([test](../tests/crash/crash_test.go#L371)).

## Identities and replay

- An **ingest idempotency key** identifies one API request for a tenant. Its exact raw request bytes must match on reuse.
- A **delivery operation ID** identifies one destination delivery and remains stable across retries and replay.
- An **attempt ID** identifies one send attempt. Several attempts can belong to one delivery.
- A **replay request key** makes the operator's replay command idempotent. Replay is allowed only for a dead delivery, opens the next delivery cycle, and keeps its operation ID ([replay transaction](../internal/store/replay.go#L27)).

An accepted event with two destinations normally creates two deliveries. A five-attempt cycle can create more than two attempts, and a receiver may see repeated requests. The demo's business-effect count stays at one per operation only when receiver deduplication is enabled.

## Evidence and limits

The project uses PostgreSQL and real HTTP/process tests for locking, crash recovery, response loss, replay races, and signing. See the [verification matrix](verification-matrix.md), [demo evidence](demo.md), [measured benchmark](benchmark.md), and [limitations](limitations.md). These prove behavior in the tested local configuration; they do not prove production availability, arbitrary receiver behavior, or a capacity target.
