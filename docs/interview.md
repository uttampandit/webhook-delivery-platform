# Interview preparation and ownership notes

This is a study guide, not proof that a candidate understands the project or independently authored every line. The implementation and these notes were created with AI coding assistance. Before using the narrative, verify each claim, replace ownership placeholders with what you personally did, and explain the design without reading the page.

## A 90-second project explanation

> This is a local Go and PostgreSQL learning project for accepting an event and delivering it independently to two synthetic webhook receivers. The API commits the event and delivery rows together before returning acceptance. A worker claims each due delivery, persists an attempt and lease, signs the stored body, sends HTTP outside the transaction, then finalizes only if its generation and claim token still match.
>
> The failure I studied is the gap after a receiver commits an effect but before the sender records acknowledgment. A real child-process test kills the worker in that window. On recovery, the same delivery is sent again: the test observes two requests but one effect because the synthetic receiver deduplicates the stable operation ID. The request can still repeat; fencing only protects database writes.
>
> I also recorded three bounded 200-event local runs. They reconciled events, attempts, and receiver effects, but they are finite closed-loop measurements, not a capacity claim. The project remains single-operator and local, has fixed destinations, and lacks production HA, automated retention, backup/restore, and public tenant isolation.

This draft intentionally avoids claiming personal ownership. Before saying “I implemented,” add the exact changes you made yourself and can explain. Be transparent that an AI coding assistant helped create the implementation and documentation; describe your own review, edits, and reasoning only where accurate.

## Five-minute architecture discussion

1. **Acceptance:** `POST /v1/events` authenticates and validates; `EventRepository.AcceptEvent` inserts the event and its requested deliveries in one PostgreSQL transaction. The same tenant/key and same raw bytes return the existing event; different bytes conflict. See [handler](../internal/api/events_create.go#L20) and [repository](../internal/store/events.go#L158).
2. **Independent delivery:** `ClaimDue` locks only ready delivery rows with `SKIP LOCKED`, inserts the `running` attempt, and commits the lease/token/generation before loading immutable send data. Each destination has its own delivery state. See [claims](../internal/store/claims.go#L35).
3. **External effect:** `HTTPSender` signs the exact persisted wire body and calls the fixed destination after the claim transaction ends. The receiver transaction records requests, dedupe keys, and effects. See [sender](../internal/delivery/http.go#L33) and [receiver](../internal/store/demo.go#L122).
4. **Uncertainty and fencing:** after response loss or process death, the reaper preserves an unconfirmed attempt and schedules recovery. A replacement changes the generation and token; a stale worker cannot finalize over the replacement. Already-issued HTTP can still repeat. See [recovery](../internal/store/recovery.go#L38), [finalization](../internal/store/finalize.go#L33), and [crash tests](../tests/crash/crash_test.go#L347).
5. **Replay and operator view:** the dashboard shows per-destination attempt/replay history and allows explicit replay of a dead delivery. Replay opens another cycle, keeps the operation ID, and reports “scheduled,” not “delivered.” See [replay store](../internal/store/replay.go#L27) and [demo evidence](demo.md).
6. **Trade-off and limit:** PostgreSQL keeps durable state and removes a separate broker from this demo, but this project is not a highly available scheduler. The shared local database and receiver scenario rows also influence the finite benchmark. See [architecture](architecture.md), [limitations](limitations.md), and [measured results](benchmark.md).

## Terms to explain in your own words

- **Lease:** temporary permission for a worker to own a delivery.
- **Fence:** generation/token checks that reject stale database writes; not a network cancellation mechanism.
- **Operation ID:** stable identity for one destination delivery across attempts and replay.
- **Unconfirmed:** no usable acknowledgment reached the sender; the remote effect may already have happened.
- **At least once:** retries may send the same logical delivery more than once. Receiver idempotency, not sender fencing, suppresses repeated effects.

## Evidence-led questions

- What exactly proves the event and both delivery rows committed before `202`?
- What survives a `SIGKILL` after a claim but before HTTP? What survives a kill after the receiver effect?
- Why does the stale finalizer fail, and why can an already-issued request still reach the receiver?
- What did the benchmark count as an event, a delivery, an attempt, a request, and an effect? Why is that run not maximum capacity?
- What would need to change before exposing arbitrary destinations or multiple customers?

Use [the exercises](../learning/EXERCISES.md) to prepare answers from current code and tests. A passing test is implementation evidence; it is not evidence that a candidate can explain the result.
