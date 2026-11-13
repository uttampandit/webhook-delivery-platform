# Failure postmortem: worker death after receiver commit

**Classification:** reproduced local crash test. This was not a production outage or a report about real customer traffic.

## Summary

A worker completed an HTTP call to a receiver that had committed its effect, then received `SIGKILL` before it could commit the attempt/delivery finalization to PostgreSQL. After the lease expired, the restarted worker retried the delivery. The receiver saw a second request with the same operation ID and its transactional dedupe ledger prevented a second effect.

## Timeline and evidence

1. The worker claims the delivery, increments the generation and attempt counters, and commits the delivery lease plus a `running` attempt before sending HTTP ([claim transaction](../internal/store/claims.go#L79)).
2. The receiver verifies the signature and commits its request and effect in one PostgreSQL transaction ([receiver transaction](../internal/store/demo.go#L122)).
3. The worker's `HTTPSender.Send` receives the response and returns. The crash test pauses at `after_send_before_finalize`, checks the receiver has one request and one effect, then kills the actual child worker with `SIGKILL` ([test](../tests/crash/crash_test.go#L347)).
4. The sender's finalization never commits, so the delivery remains `in_flight` and the first attempt remains `running`. A restarted worker's reaper marks that attempt `unconfirmed/lease_expired`, clears the old fence, and schedules a replacement attempt ([recovery transaction](../internal/store/recovery.go#L38)).
5. The retry carries the same delivery operation ID. The receiver records a second request but does not insert a second effect. The test verifies two attempts (`unconfirmed`, then `acknowledged`), two requests, one effect, and a final `succeeded` state.

The adjacent `TestPausedStaleWorkerCannotFinalizeAfterReplacementClaim` keeps the old process paused through recovery and a replacement success. On resumption, the old finalizer is rejected as stale; the receiver still records two requests and one deduplicated effect ([test](../tests/crash/crash_test.go#L371), [fenced finalizer](../internal/store/finalize.go#L54)).

## Cause, mitigation, and remaining window

The cause is a failure window between two independent transactions: receiver effect commit and sender state finalization. There is no distributed transaction spanning PostgreSQL and the remote HTTP effect. A sender-side lease and fencing token protect PostgreSQL state after recovery, but cannot undo or prevent an HTTP request that was already issued.

The implemented recovery behavior preserves the first attempt as uncertain instead of deleting it or saying it failed before send, then advances the generation and retries. Stable operation identity plus receiver-side transactional deduplication limits the synthetic receiver to one business effect. This is a mitigation, not a fix for the cross-service atomicity gap; Task 27 adds documentation, not new application code. Arbitrary receivers can still apply duplicate effects unless they deduplicate.

## Verification and remaining risk

`make test-crash` ran the actual process harness on 2026-11-12 against isolated PostgreSQL test databases and a real HTTP receiver. It passed the after-claim `SIGKILL`, after-receiver-effect `SIGKILL`, stale worker fencing, and release-build hook-isolation cases. The recorded output is [the Task19 crash log](../artifacts/demo/crash/task19-make-test-crash.log); the scenario and database assertions are in [crash_test.go](../tests/crash/crash_test.go#L326).

This experiment proves the expected local state transition and effect count for the synthetic deduplicating receiver. It does not prove exactly-once delivery, behavior of third-party receivers, process death on a production host, high availability, or disaster recovery. See [limitations](limitations.md).
