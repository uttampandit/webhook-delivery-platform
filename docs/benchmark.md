# Finite local benchmark

`make benchmark` runs a bounded, finite ingest workload against the already-running local Compose services. It creates one normal, deduplicating receiver scenario per run, sends unique idempotency keys to both fixed destinations, then waits for accepted deliveries to succeed or become dead before a single run-wide deadline. It does not retry a failed ingest request with a new key.

## Run it

Initialize `.env` once and start the local runtime with `make up`; the benchmark command requires the API, worker, PostgreSQL, and both receivers to already be healthy. It runs as a one-off Compose container on the internal network, connects only to the local PostgreSQL service, and writes reports under `artifacts/bench/<run-id>/`.

```sh
make benchmark \
  BENCHMARK_RUN_ID=task26-example \
  BENCHMARK_EVENTS=200 \
  BENCHMARK_PRODUCER_CONCURRENCY=4 \
  BENCHMARK_PAYLOAD_BYTES=1024 \
  BENCHMARK_TIMEOUT=180s
```

Flags are checked by the command as well as by the Make target. Events are limited to 10,000, producer concurrency to 32, payload target to 32,768 bytes, and the whole-run timeout to 30 minutes. The defaults are 200 events, concurrency 4, a 1,024-byte serialized ingest request, and a 180-second timeout. A run ID may contain 1–40 ASCII letters, digits, underscores, or hyphens; artifact directories are never overwritten.

`payload-bytes` means the exact complete serialized API request body, before the API builds the outgoing receiver envelope. The benchmark pads `payload.padding` with ASCII `x` bytes and reports the input request size separately from the stored wire body size. A target smaller than the minimum required metadata is rejected with the measured minimum size.

## What the report measures

Every report includes the exact command arguments and effective configuration, host and process OS/architecture, CPU/RAM, host Go and Node versions, PostgreSQL server version, source commit plus dirty-worktree status, application/PostgreSQL image IDs, Docker CPU/memory limits, worker concurrency/poll/lease/timeout settings, database pool sizes, and the keepalive setting. It never writes API tokens, receiver control tokens, or database URLs.

The report stores sanitized per-request samples and per-delivery samples as CSV beside `report.json`. Client latency uses Go's monotonic elapsed clock. Durable event, delivery, attempt, and receiver counts and timestamps come from read-only PostgreSQL queries scoped to this run's idempotency keys and scenario ID. The report compares returned event and delivery IDs with PostgreSQL, confirms two configured receiver scenarios, and checks receiver operation IDs and deduplicated effects against the run's deliveries.

Percentiles use the nearest-rank method: sort the samples and select one-based rank `ceil(p*n)`. Client latency reports all attempted requests and valid HTTP 200/202 responses separately. Accepted unique events per second is the PostgreSQL-verified unique event count divided by monotonic ingest-phase duration. Initial queue delay is the first persisted attempt start minus event creation time; acknowledgment latency is first confirmed delivery time minus event creation time for succeeded deliveries only. Retry amplification is persisted attempt rows divided by unique deliveries. Receiver request amplification is persisted receiver request rows divided by unique deliveries. Dead, pending, in-flight, and unstarted work stays visible in its own counts and denominator.

The benchmark is a finite closed-loop workload, not a maximum sustainable throughput test. All services share one local host, PostgreSQL instance, and receiver scenario rows, so this report describes the recorded configuration and run only. Receiver row-lock contention is part of this demo's measured path. An attempt row may exist even if the worker dies before sending HTTP.

## Measured evidence

No performance target is asserted. One warm-up used 20 events, producer concurrency 2, and a 1,024-byte request; it completed and is excluded from the measured-run comparisons. Three measured runs used 200 events each, producer concurrency 4, a 1,024-byte request, and a 180-second whole-run timeout.

| Run | DB-verified unique events/sec | API latency p95, success-only (ms) | Initial queue delay p95 (ms) | End-to-end acknowledgment p95 (ms) | Reconciliation |
|---|---:|---:|---:|---:|---|
| [Run 1 report](../artifacts/bench/task26-measured-1-20261112/report.json) | 2,962.906 | 1.981 | 251.850 | 253.170 | Passed |
| [Run 2 report](../artifacts/bench/task26-measured-2-20261112/report.json) | 2,572.812 | 2.964 | 367.168 | 368.578 | Passed |
| [Run 3 report](../artifacts/bench/task26-measured-3-20261112/report.json) | 2,564.771 | 2.594 | 327.434 | 328.712 | Passed |

All three runs accepted 200 unique events, created 400 deliveries, persisted 400 attempts and receiver requests, and recorded 400 receiver effects. All 400 deliveries succeeded in each run; there were no dead or unfinished deliveries, no failed API requests, and no lost acknowledgments. Retry amplification and receiver request amplification were both 1.0. The input body was exactly 1,024 bytes; the stored outgoing wire body ranged from 1,015 to 1,018 bytes.

Accepted-event rate ranged from 2,564.771 to 2,962.906 events/second across the three runs, a 398.135 events/second range (15.47% of the median). This is run-to-run variation for this finite local workload, not a sustainable-capacity estimate.

The host was macOS 27.2 arm64 with 10 reported CPU cores and 16 GiB RAM; the benchmark process ran Linux arm64 with Go 1.27.3. Host Go was 1.27.3, Node was 26.13.0, and PostgreSQL was 17.11. Worker settings were concurrency 4, poll interval 250 ms, lease 30 s, lease margin 5 s, HTTP timeout 5 s, and database statement timeout 3 s. The configured worker pool was 6 connections; API and receiver pools were 8 each; the benchmark pool was 2; API and worker metrics pools were 1 each. HTTP keepalive was disabled for the benchmark client and worker sender. Docker reported no per-container CPU or memory limits for these services (zero means no limit configured).

At measurement time, Git `HEAD` was `45639d2d5148429ae4154cfe470994cd50efde5d` and the worktree was dirty because Task 26 changes were still under review. The running API, worker, and receivers used image ID `sha256:a9b349342d3158a837fcc3e968e5bfd3583fdc603c54689d42153a9b424d633d`; PostgreSQL used `sha256:839fbb5a4942a7758dd93e18b42ea5649807491a0545b5eeeb8f0b8bb11857a5`. Each one-off benchmark image ID and the complete environment are preserved in the linked run reports.

Raw CSV samples:

- Warm-up: [report](../artifacts/bench/task26-warmup-20261112/report.json), [ingest samples](../artifacts/bench/task26-warmup-20261112/ingest_samples.csv), [delivery samples](../artifacts/bench/task26-warmup-20261112/delivery_samples.csv)
- Run 1: [ingest samples](../artifacts/bench/task26-measured-1-20261112/ingest_samples.csv), [delivery samples](../artifacts/bench/task26-measured-1-20261112/delivery_samples.csv)
- Run 2: [ingest samples](../artifacts/bench/task26-measured-2-20261112/ingest_samples.csv), [delivery samples](../artifacts/bench/task26-measured-2-20261112/delivery_samples.csv)
- Run 3: [ingest samples](../artifacts/bench/task26-measured-3-20261112/ingest_samples.csv), [delivery samples](../artifacts/bench/task26-measured-3-20261112/delivery_samples.csv)

Exact measured commands:

```sh
make benchmark BENCHMARK_RUN_ID=task26-warmup-20261112 BENCHMARK_EVENTS=20 BENCHMARK_PRODUCER_CONCURRENCY=2 BENCHMARK_PAYLOAD_BYTES=1024 BENCHMARK_TIMEOUT=180s
make benchmark BENCHMARK_RUN_ID=task26-measured-1-20261112 BENCHMARK_EVENTS=200 BENCHMARK_PRODUCER_CONCURRENCY=4 BENCHMARK_PAYLOAD_BYTES=1024 BENCHMARK_TIMEOUT=180s
make benchmark BENCHMARK_RUN_ID=task26-measured-2-20261112 BENCHMARK_EVENTS=200 BENCHMARK_PRODUCER_CONCURRENCY=4 BENCHMARK_PAYLOAD_BYTES=1024 BENCHMARK_TIMEOUT=180s
make benchmark BENCHMARK_RUN_ID=task26-measured-3-20261112 BENCHMARK_EVENTS=200 BENCHMARK_PRODUCER_CONCURRENCY=4 BENCHMARK_PAYLOAD_BYTES=1024 BENCHMARK_TIMEOUT=180s
```
