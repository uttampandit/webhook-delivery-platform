# Current limitations

This repository is a local, synthetic learning project. The checks and benchmark establish behavior only for the tested code, Compose configuration, PostgreSQL instance, and local receiver processes.

## Product and operations

- One operator and a configured demo tenant; no customer registration, billing, public destination management, tenant onboarding, or production tenant isolation.
- Destinations are fixed to fulfillment and notification. There is no public UI/API for arbitrary customer endpoints and no third-party provider ingestion workflow.
- The full local runtime requires one Compose host and one PostgreSQL database. No HA/failover, automated backup/restore, disaster recovery, zero-downtime deployment, or production support/SLA is provided.
- API/control tokens and signing keys come from a local `.env`. The dashboard holds its API token in memory. There is no public identity system, RBAC, secret-manager integration, or production TLS/ingress configuration here.
- Events, request bodies, idempotency keys, attempts, replay records, and synthetic receiver ledgers have no automated retention/cleanup policy. `make down` preserves the database volume.
- There is no broker, outbox relay, general destination reconciliation endpoint, arbitrary delivery rate control, or customer alerting workflow.

## Delivery guarantees

- Delivery is at least once. A timeout, connection reset, response loss, or process death can leave an effect uncertain and cause another HTTP request. A generation/token/lease fences stale PostgreSQL writes; it cannot cancel a request already sent.
- Only receiver-side transactional idempotency can suppress a duplicate business effect. The example receiver demonstrates it; external receivers must implement their own idempotency contract.
- Automatic attempts are bounded to five per delivery cycle. An exhausted delivery is visible as dead and can be explicitly replayed into a new cycle. No retry policy guarantees that an external endpoint eventually accepts an event.
- A successful HTTP acknowledgment proves the receiver returned a success response, not that an external business workflow completed beyond that receiver's own contract.

## Measurements and evidence

- `make benchmark` is a finite closed-loop workload on a shared local host. It does not measure open-loop bursts, maximum sustainable throughput, multi-host scaling, or production capacity. It uses normal local receivers and the same PostgreSQL database, so local lock contention is part of the measured path.
- Benchmark results vary with host, image, database contents, worker settings, and runtime load. The three recorded accepted-event rates are evidence about those runs only; they are not an SLO or performance promise.
- Worker/API runtime counters are process-local and reset on restart. PostgreSQL event/attempt/replay records remain the durable audit source, subject to the no-retention limitation above.
- Test hooks for process checkpoints are compiled only in integration/crash-test builds; the release worker ignores the hook environment variables. Crash coverage uses real child worker processes and a real HTTP receiver backed by PostgreSQL, not production deployment infrastructure.

For the exact measured configuration, see [benchmark evidence](benchmark.md). For the reproduced response-loss case, see the [failure postmortem](failure-postmortem.md).
