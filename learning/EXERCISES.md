# Understanding checks

Current implementation checkpoint: Task 27. These are still questions for the learner to answer; completed implementation and passing tests do not demonstrate that a candidate understands the answers. See the [README](../README.md), [architecture guide](../docs/architecture.md), and [interview study guide](../docs/interview.md).

Do these in a disposable test database or fresh scenario. Do not mutate real/demo persisted deliveries manually to get a better recording. The coding agent can guide the user and explain answers, but should let the user attempt the reasoning first.

1. **Trace identity:** Given an event ID, find its two delivery IDs, operation keys and first attempts. Which values change on retry? Which change on replay? Predict before inspecting.
2. **Byte identity:** Send the same JSON with different whitespace under the same ingest key. Predict status and row counts. Explain why v1 deliberately does not canonicalize.
3. **Fan-out:** Keep fulfillment503 and notification healthy. Predict each row's lifecycle and whether notification repeats just because fulfillment retries.
4. **Lost response:** Enable commit_drop_once with dedupe on. Predict requests/effects/outcomes. Repeat in a new scenario with dedupe off. Locate the transaction that changes the result.
5. **Stale worker:** Pause after claim, let recovery occur, then resume. Which DB mutation must fail? Why might a receiver still see another request? What could stop duplicate effects?
6. **Replay race:** Two distinct replay keys target one dead delivery. Predict audit count/cycle. Then retry the winning key after success and explain why it still returns the old replay response.
7. **Budget:** Kill a worker before send on five claims. Why can the delivery become dead without five HTTP requests? Explain the policy trade-off honestly.
8. **Signature:** Change one payload byte, then change operation ID while leaving signature. Predict verification. Why does a timestamp window not replace dedupe?
9. **Diagnosis:** A delivery is dead with earlier HTTP 500 and unconfirmed attempts. List facts you know and what you cannot infer about external effects. Explain what real reconciliation would require.
10. **Small code change:** Add a test for an untested Retry-After edge case, explain it, run it. Alternatively add a harmless destination-code display improvement and verify it. Do not change retry identity/lease semantics as an exercise.
11. **Measurements:** Explain why 200 accepted events can create400 deliveries and more than400 attempts while still producing400 total effects with dedupe. Reconcile an actual run.
12. **Scaling decision:** Given actual pool saturation/queue-delay data, propose one small experiment. Name what would justify a broker and what adding one would complicate. Do not add one just for the exercise.
13. **Competitor scope:** Use the [capability map](../docs/competitors.md) to compare one documented vendor feature with this project's actual scope. Explain why an `Unknown` cell is not proof that the vendor lacks that feature, and why a feature list does not establish quality or commercial novelty.

Evidence pointers: [`events.go`](../internal/store/events.go), [`claims.go`](../internal/store/claims.go), [`worker.go`](../internal/delivery/worker.go), [`recovery.go`](../internal/store/recovery.go), [`finalize.go`](../internal/store/finalize.go), [`replay.go`](../internal/store/replay.go), [real crash tests](../tests/crash/crash_test.go), [benchmark reports](../docs/benchmark.md), and [demo reports](../docs/demo.md). Record the learner's own answers and remaining questions in a separate learning note; do not fill them in based on test outcomes. The objective is accurate understanding, not memorizing impressive terms.
