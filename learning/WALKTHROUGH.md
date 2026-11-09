# Explain the project as you build it

Current checkpoint: backend through Task 18. Later SIGKILL, dashboard and benchmark sections below are planned exercises, not completed demonstrations. See the [README](../README.md) for available commands.

Read the matching section after the named task. The implementation agent should answer using the actual source and tests; these are expected explanations, not a claim of candidate mastery.

## After07 — acceptance boundary

Trace one authenticated request through validation, raw-byte hashing and the ingest transaction. Identify the exact commit before202. What can the client know if its response is lost? Why does retrying the same key/bytes resolve that uncertainty? Show the unique constraint and the separate conflict-path SELECT. Explain why a process-local map or an external queue publish after commit changes the failure boundary.

## After13 — one delivery

Trace event→delivery→claim→running attempt→signed POST→receiver ledger/effect→finalization. Show actual IDs and stored bytes. Explain why HTTP runs after the claim transaction ends. Show the stable operation ID versus per-attempt timestamp/signature. Why can one destination fail while the other succeeds? Explain why a persisted attempt does not prove the request was sent.

## After19 — failure and recovery

Show the two actual crash test outputs. Draw the lost-response/crash window between external effect and sender finalization. Explain what the lease permits, why a generation/token is needed, and exactly what it does not prevent. Show a stale write rejected while an extra HTTP request remains possible. Explain dedupe enabled/off counts and why idempotency belongs in the receiver's business transaction.

## After22 — operator experience

Use the UI to inspect one dead delivery, its earlier uncertain attempts and replay audit. Explain why a replay keeps the operation ID and why successful-operation resend is a distinct omitted feature. Demonstrate preserving ingest bytes/key after a failed browser request. Explain why an API token is held in memory and what the single-operator security limit is.

## After26 — measurements

Run a small benchmark and explain every numerator/denominator. Distinguish accepted events, delivery rows, persisted attempts, actual receiver requests and effects. Why is the finite closed-loop run not a maximum throughput claim? Point to actual raw samples, environment and limits. Identify one measured bottleneck before discussing an optimization.

## Final interview narrative

Prepare a 90-second introduction and a five-minute architecture discussion using current evidence. A useful sequence is: synthetic user outcome; acceptance transaction; independent deliveries; one difficult failure experiment; trade-off; measured result; honest limitation. Include AI assistance honestly if asked. Do not imply you independently designed/coded every line if that is inaccurate.

Expected trade-off discussions: PostgreSQL queue versus a separate broker; fixed destinations versus general egress controls; bounded retries versus guaranteed delivery; lease without heartbeat versus long-running jobs; receiver-local transaction versus distributed exactly-once claims; local single operator versus production SaaS; retained keys/ledgers versus cleanup complexity.

Create docs/interview.md only from actual code/results. “I haven't implemented that” is a valid answer about HA, disaster recovery, destination management, multi-tenancy or public auth.
