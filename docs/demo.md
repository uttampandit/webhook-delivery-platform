# Failure walkthrough and evidence

This guide runs the synthetic delivery stories against the local Compose stack. It sends requests only to the two local receivers; it does not change delivery rows directly, erase prior data, or contact a real fulfillment or notification service.

## Fresh local run

Prerequisites: Docker with the Compose plugin, Go and Node/npm for the repository build, and a local `.env` created with `make init-env` when one does not already exist. Keep `.env` private. Start the services and run all five stories:

```sh
make up
make demo
```

`make demo` takes about 18–20 seconds on the tested Compose setup. It creates a new run ID and new receiver scenario IDs, publishes five distinct events, waits for real delivery states, reads attempt history from the API, and inspects receiver request/effect counts. It writes a sanitized report to `artifacts/demo/<run-id>/report.json`; a failed expectation exits nonzero and saves the partial report.

Open [http://127.0.0.1:8080](http://127.0.0.1:8080), enter `API_TOKEN` privately, and inspect an event ID from the report. The token stays in browser memory. Pause screen recording while entering it, and never include it in a command transcript, screenshot, URL, or report. Stop the normal runtime with `make down`; its PostgreSQL volume stays in place.

## Stories and checked outcomes

The fresh-volume report is [demo_20261112T084454Z_29c3f4e31768](../artifacts/demo/demo_20261112T084454Z_29c3f4e31768/report.json). All five scenarios passed. The second report, [demo_20261112T084528Z_f826c0717306](../artifacts/demo/demo_20261112T084528Z_f826c0717306/report.json), also passed on the same volume with five new scenario IDs and five new event IDs; no data was deleted between runs.

| Story | Event ID from the fresh-volume run | Persisted result |
|---|---|---|
| Healthy fan-out | `1098b4b4-c692-4935-8267-e6b8cda108e4` | Fulfillment and notification each recorded 1 request and 1 effect; both deliveries succeeded. |
| Outage isolation and recovery | `f64c17bf-f5a2-4dbf-a7a2-a41d79bc4c52` | Notification succeeded while fulfillment recorded HTTP 503; fulfillment recovered on attempt 2. Counts: fulfillment 2/1 requests/effects, notification 1/1. |
| Lost response, dedupe on | `4f50b898-6384-4dd1-adbe-49b61ee002cd` | Two requests, one effect, and an earlier unconfirmed attempt. The live dashboard also showed this uncertainty. |
| Lost response, dedupe off | `2d376909-6af4-4d1b-bcdd-0942b3fff490` | Two requests and two effects. This is deliberate unsafe receiver behavior, not a recommended setting. |
| Dead delivery and replay | `d48ffde7-1fc3-4860-9fbb-eaddd6c08412` | Five HTTP 503 attempts ended cycle 1. Replay `017e5bbf-cd2c-4e0d-94ca-c2fc648c76b7` kept the operation ID, succeeded in cycle 2 at lifetime attempt 6, and left one effect after six requests. |

On the second run, the response-loss event `407124f5-268b-4cdf-a8ab-b096d746068e` displayed its full event ID and the “Has unconfirmed attempts — effect may have occurred” notice. The delivery detail also showed attempt 1 as unconfirmed and attempt 2 as acknowledged.

An **unconfirmed attempt** means the sender did not receive a usable response; the receiver may already have committed its effect. The **operation ID** is the stable delivery identity reused for retries and replay. Sender retries cannot guarantee exactly-once effects; in this demo, the receiver's PostgreSQL dedupe ledger suppresses the repeated effect when dedupe is enabled.

The outage recovery can occasionally reach `dead` before an operator observes the pending retry. In that case `democtl run` switches the receiver back to normal, explicitly replays the delivery, and records the fallback in the report. The deterministic PostgreSQL integration test separately verifies the before-exhaustion path: it persists the first real HTTP 503 as pending, recovers the receiver, advances only the isolated test row's `next_attempt_at`, claims and sends the second attempt, and checks both persisted delivery history and receiver counts.

## Recording

After `make up`, run:

```sh
bash scripts/demo-recording.sh
```

The helper runs `make demo` and stores its unedited command/output transcript under `artifacts/demo/recording-*/transcript.txt`. The captured [Task25 transcript](../artifacts/demo/recording-20261112T085211Z-89697/transcript.txt) shows one complete fresh run. For a short screen walkthrough, use the saved report IDs and clearly label any cuts around actual retry/backoff waits. The command transcript remains unedited; do not alter reported counts to shorten the story. The worker-death demonstration is separate because it kills a worker process rather than interrupting a UI request.

## Worker-death evidence

Run the isolated Task19 process harness separately:

```sh
make test-crash
```

The actual 2026-11-12 output is saved in the [Task19 crash test log](../artifacts/demo/crash/task19-make-test-crash.log); it reports `ok github.com/uttampandit/webhook-delivery-platform/tests/crash (17.131s)`. The harness kills child workers after claim commit and after the receiver commits its effect, then checks real PostgreSQL recovery, repeated request counts, receiver dedupe, and stale-worker fencing. Release Compose workers do not include crash hooks.

## Tested setup and limitation

Verified on macOS 27.2, Apple arm64 (`Mac17,3`), 10 CPU cores and 16 GiB host memory, with Docker Engine 29.9.2 and Compose 5.6.1, Go 1.27.3, Node 26.13.0 and npm 11.20.0. Compose ran the pinned PostgreSQL 17.11 image. The walkthrough uses real retry backoff and can take longer when the worker or local machine is slow. It does not make a benchmark claim.

The trade-off is that demo scenarios and their event/attempt/effect records remain in the database so the reports can point to durable evidence. Repeated demos use new IDs and grow the local volume; `make demo` never cleans it up.
