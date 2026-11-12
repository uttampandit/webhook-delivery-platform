# Logs and runtime metrics

The API, worker, receiver, seed and migration commands use JSON `slog` output. The logger retains a fixed message set and the bounded fields `request_id`, `event_id`, `delivery_id`, `attempt_id`, `cycle_no`, `attempt_no`, `generation`, `outcome`, `http_status`, `error_code`, `duration_ms`, `scheduled_delay_ms`, `worker_id`, and `component`. IDs must be canonical UUIDs; outcomes, components and error codes must match fixed enums. Unknown fields, nested groups, raw errors, request paths, URLs, headers, payloads and connection strings are dropped or replaced with a safe value.

The pinned Prometheus Go client is `github.com/prometheus/client_golang v1.25.0`. Each API and worker process exposes a private `/metrics` endpoint. In host mode the API listens at `127.0.0.1:9090` and the worker at `127.0.0.1:9091`; in Compose mode each listens on its own container's port `9090`. Do not publish or map these ports to the host.

| Metric | Type and labels | Meaning |
|---|---|---|
| `webhook_ingest_total{result}` | Counter; `accepted`, `deduplicated`, `conflict`, `invalid`, `error` | API `POST /v1/events` outcomes, including validation and authorization failures. |
| `webhook_ingest_duration_seconds` | Histogram; seconds | Time handling the event POST, including its response write. |
| `webhook_attempts_total{outcome}` | Counter; `acknowledged`, `http_failure`, `unconfirmed`, `local_failure` | Finalizations confirmed by this process plus attempts recovered by the worker. |
| `webhook_deliveries_total{result}` | Counter; `succeeded`, `dead` | Terminal delivery transitions observed by this process. |
| `webhook_stale_finalization_total` | Counter | Database finalizations rejected by the generation/token/lease fence. |
| `webhook_queue_due` | Gauge | Tenant-scoped pending deliveries due for a claim. |
| `webhook_in_flight` | Gauge | Tenant-scoped deliveries currently claimed. |
| `webhook_dead` | Gauge | Tenant-scoped dead deliveries. |
| `webhook_queue_delay_seconds` | Histogram; seconds | PostgreSQL claim timestamp minus scheduled due time, clamped at zero. |
| `webhook_delivery_latency_seconds` | Histogram; seconds | Event creation to the first successful acknowledgment; dead outcomes are excluded. |
| `webhook_gauge_collection_failures_total` | Counter | Failed or timed-out database gauge refreshes. |

All histograms use the explicit buckets `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60` seconds. Metrics have no ID, URL, payload or credential labels. Success and dead-delivery counts are available beside delivery latency in `webhook_deliveries_total`.

Gauge refresh runs every 15 seconds with a one-second context and a dedicated one-connection pool for each API/worker process. A failed refresh increments the failure counter and leaves the last successful values in place. The regular pool is capped at 8 API connections, `worker concurrency + 2` worker connections, 8 receiver connections, and 2 connections for seed/migration commands. A gauge query does not lock scheduling rows.

Runtime counters reset on process restart and may miss activity at crash or ambiguous-commit boundaries. A worker counter describes only outcomes that worker process observed; it is not proof of receiver-side effects. Use PostgreSQL event, delivery, attempt, replay, and receiver request/effect tables as the durable audit record. These metrics are operational signals, not benchmark measurements or end-to-end exactly-once evidence.

For host-mode local inspection, run `curl -fsS http://127.0.0.1:9090/metrics` for the API or `curl -fsS http://127.0.0.1:9091/metrics` for the worker.
