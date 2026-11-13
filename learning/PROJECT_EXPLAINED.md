# Understand the project before reading the code

This guide explains the current design through Task 27. The dashboard, full Compose runtime, actual worker-death tests, failure demos, and finite benchmark are implemented and linked from the [README](../README.md). Passing implementation tests does not prove a learner understands these explanations.

## Start with one order

A synthetic shop confirms order `order_42`. Two applications need to know: fulfillment should record that the order needs packing, and notification should record a notification action. In this demo those actions are database entries, not physical shipments or real messages.

A **webhook** is an HTTP request one application sends to another when something happens. Here the shop submits an event to our API, and our worker sends webhook requests to the two receiver applications.

Why put our platform in the middle? It gives the operator a durable record of accepted events, per-destination delivery history, controlled retries, and recovery tools. Existing products offer this category of service. Our portfolio contribution is an implementation we can inspect, test and explain.

## The pieces and their responsibilities

| Piece | What it does | Why we need it |
|---|---|---|
| API, written in Go | Validates and stores an event; exposes query/replay endpoints | Gives the publisher and dashboard a clear interface |
| PostgreSQL | Stores events, delivery jobs, attempts and replay history | Work remains recorded when a process stops |
| Worker, written in Go | Claims due delivery jobs and sends signed HTTP requests | Delivery/retry work runs independently of the incoming API request |
| Two demo receivers | Validate signatures and record synthetic business effects | Let us reproduce both safe and unsafe receiver behavior |
| React dashboard | Lets the local operator inspect events, per-destination states, attempts, and dead-delivery replay | Makes the synthetic system and its uncertain outcomes visible |
| Docker Compose | Runs PostgreSQL, API, worker, dashboard, and both synthetic receivers locally | Makes the demo repeatable without a cloud account |

PostgreSQL also holds the local receivers' demo tables for convenience. In a real integration, each receiver would normally own its own database. This demo does not claim a distributed transaction across sender and receivers.

## One event, two deliveries, several attempts

These words mean different things:

- **Event:** the recorded fact that `order_42` was confirmed.
- **Delivery:** the job of sending that event to one destination. Two destinations mean two deliveries.
- **Attempt:** one claimed try at a delivery. It is recorded before HTTP starts, so a crash may leave an attempt that never reached the receiver.
- **Request:** an HTTP request that actually reached a receiver. Persisted attempt count and observed receiver request count are different measurements.
- **Business effect:** the receiver's recorded action, such as adding the fulfillment entry. Repeated requests should not necessarily repeat that action.

Suppose fulfillment returns HTTP 503 while notification returns HTTP 204. Notification can finish successfully; fulfillment retries independently. We do not resend the notification merely because fulfillment is having trouble.

## What “accepted” means

The API stores the event and both delivery jobs in one database **transaction**: either they all commit together or they do not. Only after commit does it return HTTP 202 Accepted.

Accepted means “stored for processing.” It does not mean the receiver has completed its action. If the API's response is lost after commit, the caller may not know whether acceptance happened. An incoming idempotency key resolves this: retrying the same key with exactly the same request bytes returns the original event instead of creating another one.

In this version a key is retained indefinitely and scoped to the tenant. The same key with different bytes returns a conflict, including changes to whitespace. That is a deliberate, simple contract, not a claim that two JSON formatting styles represent different business meaning.

## The failure that makes the project interesting

Two different response-loss cases are implemented and tested with real HTTP and PostgreSQL. The local demo closes a receiver connection after its effect commits. A separate process harness kills the actual worker after its HTTP response but before sender finalization; see the [failure postmortem](../docs/failure-postmortem.md). Neither is a production outage.

1. The worker sends the fulfillment webhook.
2. The receiver records the fulfillment effect and commits it.
3. The receiver's connection closes before a response reaches the worker.
4. The worker sees no valid response. It cannot know whether the effect happened.
5. A later attempt sends the same operation again.

With **receiver deduplication enabled**, the receiver keeps a durable ledger of operation IDs. Its first ledger entry and business effect commit in the same local transaction. On a repeated operation/body, it acknowledges without adding another effect. Two observed requests can therefore produce one effect.

With deduplication disabled in a separate demo scenario, both requests produce effects. The comparison shows the sender's limit: retrying cannot by itself guarantee exactly-once external effects. Even a non-success response cannot always prove a real receiver made no change.

The dropped-response experiment must actually close a connection after commit. Returning HTTP 503 before the effect is a different experiment.

## How workers coordinate

A worker **claims** due work for a limited period called a **lease**. Claiming writes a running attempt and a fresh token/generation before the worker sends anything. Another worker skips rows currently locked by a short claim transaction, so workers can share the queue without waiting on every same row.

The worker does not hold a database transaction open during HTTP. When it finishes, it can update the delivery only if its claim still matches and its lease was valid at the protected check. This check is called **fencing**: an old worker cannot overwrite a newer worker's database state.

If a worker dies, a recovery loop notices the expired lease. It preserves the old attempt as unconfirmed and schedules another try if the cycle has budget left. A paused old process may later resume and still send a request. Fencing blocks its stale database update; receiver deduplication is what protects the repeated business effect.

## Retries, dead deliveries and replay

We retry network uncertainty, HTTP 429 and HTTP 5xx. Other non-2xx statuses end the cycle in this version. Waits grow with bounded randomness so many failures do not all retry at exactly the same time. A valid Retry-After on 429/503 can extend the wait within the configured cap.

Each cycle allows five claims, including claims abandoned before sending. After the budget ends, or a permanent failure occurs, the delivery becomes **dead**. Dead means the cycle ended; it does not prove no external effect happened.

An operator can fix the receiver and **replay** a dead delivery. Replay creates a new retry cycle with an audit record. The operation ID stays the same, so a receiver that already applied the operation can still deduplicate it. An intentional new send of a successful operation is a different feature and is outside this version.

## Delivery states: available through the local dashboard and API

| Display | Meaning |
|---|---|
| Pending | Waiting for its first or next claim |
| In flight | A worker holds a claim; HTTP may or may not have begun |
| Succeeded | A valid worker finalization recorded a receiver's 2xx acknowledgement |
| Dead | This cycle ended and needs operator attention |
| Unconfirmed attempt | The sender did not confirm the outcome; an effect may exist |

Earlier unconfirmed attempts remain visible after a later success. A currently running attempt stops contributing to uncertainty if that same attempt is later acknowledged; a completed unconfirmed attempt remains in history.

## Why these technologies?

Go provides explicit HTTP behavior, cancellation and concurrent workers. PostgreSQL gives us atomic storage and row locking without introducing a second queue system. React makes the operational timeline easy to use. Compose provides a local reproducible environment. The project does not need an AI feature to demonstrate useful engineering in an AI-heavy job market.

Kafka or Redis could be discussed as alternatives later. Adding another system now would introduce coordination and operational work before measurements justify it. The [three finite benchmark runs](../docs/benchmark.md) provide bounded local evidence, not a maximum-throughput or scale result.

## Learn in small stages

After Task 07, explain how acceptance avoids duplicate ingest. After Task 13, trace one signed delivery. After Task 19, explain worker death and the duplicate-effect window. After Task 22, use the dashboard and replay. After Task 26, explain measured results and their limits. Task 27 adds the sourced [competitor map](../docs/competitors.md) and [interview study guide](../docs/interview.md); use them to guide research and practice, not as evidence of personal understanding.

Use [WALKTHROUGH.md](WALKTHROUGH.md) for those milestones, [EXERCISES.md](EXERCISES.md) for experiments, and an optional local `learning/LEARNING_LOG.md` notebook for questions (not published). You should eventually be able to trace the code and make a small change; memorizing this explanation is not the same as understanding the implementation.
