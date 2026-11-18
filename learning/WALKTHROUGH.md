# Request and recovery walkthrough

Follow an event from acceptance to delivery, then inspect the failure boundaries using the linked implementation and recorded tests. See the [README](../README.md) for runtime commands and the [implementation overview](../docs/interview.md) for design decisions.

## Event acceptance

An authenticated request passes validation and raw-byte hashing before the ingest transaction stores the event and its deliveries. HTTP 202 follows the commit. If the response is lost, retrying the same key and bytes retrieves the original event. Different bytes conflict. See [event acceptance](../internal/store/events.go).

## Signed delivery

The worker claims a delivery, persists a running attempt, and commits before sending signed HTTP. The operation ID stays stable across retries; timestamps and signatures belong to individual requests. Destinations progress independently. A persisted attempt does not prove the request reached its receiver. See [claims](../internal/store/claims.go) and [worker](../internal/delivery/worker.go).

## Failure and recovery

A worker can die after claim or after receiver commit. Once its lease expires, recovery preserves the attempt as unconfirmed and schedules another claim when budget remains. Generation and token checks reject stale finalization. An already-issued request can still reach the receiver, so receiver-side deduplication must share the business-effect transaction. See [crash tests](../tests/crash/crash_test.go) and the [failure postmortem](../docs/failure-postmortem.md).

## Operator recovery

The dashboard shows dead deliveries, earlier uncertain attempts, and replay history. Replay opens a new cycle and preserves the operation ID. An uncertain event submission preserves its exact bytes and ingest key for retry. The API token stays in memory and must be entered again after a refresh. See [local operations](../docs/operations.md).

## Measurements and trade-offs

The [benchmark report](../docs/benchmark.md) distinguishes accepted events, deliveries, attempts, receiver requests, and effects. The finite workload does not establish maximum sustainable throughput. PostgreSQL provides durable scheduling alongside event storage, while the single host, fixed destinations, bounded attempts, and retained records define the current operational limits. See [architecture](../docs/architecture.md), [limitations](../docs/limitations.md), and the [capability map](../docs/competitors.md).
