#!/bin/sh

set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"

if ! docker compose version >/dev/null 2>&1; then
	printf '%s\n' 'Docker Compose v2 is required for the local benchmark.' >&2
	exit 2
fi

# Keep the benchmark attached to the existing local runtime. It does not start,
# replace, or reset application services or PostgreSQL volumes.
python3 - <<'PY'
import json
import subprocess
import sys

required = ("api", "worker", "fulfillment", "notification", "postgres")
for service in required:
    result = subprocess.run(
        ["docker", "compose", "ps", "-q", service],
        text=True,
        capture_output=True,
    )
    container_id = result.stdout.strip()
    if result.returncode or not container_id:
        print("Local Compose service is missing: " + service, file=sys.stderr)
        print("Start the isolated local runtime with make up, then rerun the benchmark.", file=sys.stderr)
        raise SystemExit(2)
    inspected = subprocess.run(
        ["docker", "inspect", container_id], text=True, capture_output=True
    )
    try:
        container = json.loads(inspected.stdout)[0]
    except (IndexError, json.JSONDecodeError):
        print("Could not inspect local Compose service: " + service, file=sys.stderr)
        raise SystemExit(2)
    state = container.get("State", {})
    health = state.get("Health", {}).get("Status")
    if state.get("Status") != "running" or health != "healthy":
        print("Local Compose service is not healthy: " + service, file=sys.stderr)
        raise SystemExit(2)
PY

docker compose --profile benchmark build benchmark
mkdir -p artifacts/bench

host_metadata=$(python3 - <<'PY'
import json
import os
import platform
import subprocess

def text_command(args):
    try:
        result = subprocess.run(args, text=True, capture_output=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable"
    return result.stdout.strip() if result.returncode == 0 else "unavailable"

memory = 0
raw_memory = text_command(["sysctl", "-n", "hw.memsize"])
try:
    memory = int(raw_memory)
except ValueError:
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        memory = 0

head = text_command(["git", "rev-parse", "HEAD"])
dirty = bool(text_command(["git", "status", "--porcelain=v1"]))
host = {
    "platform": platform.platform(aliased=True),
    "os": platform.system(),
    "release": platform.release(),
    "architecture": platform.machine(),
    "cpu_count": os.cpu_count() or 0,
    "memory_bytes": memory,
    "go_version": text_command(["go", "version"]),
    "node_version": text_command(["node", "--version"]),
    "source_commit": head if head != "unavailable" else "",
    "worktree_dirty": dirty,
}
print(json.dumps(host, separators=(",", ":")))
PY
)

runtime_metadata=$(python3 - <<'PY'
import json
import subprocess
import sys

def run(args):
    return subprocess.run(args, text=True, capture_output=True, check=True)

def container_info(service):
    container_id = run(["docker", "compose", "ps", "-q", service]).stdout.strip()
    item = json.loads(run(["docker", "inspect", container_id]).stdout)[0]
    host = item.get("HostConfig", {})
    config = item.get("Config", {})
    return {
        "image_ref": config.get("Image", ""),
        "image_id": item.get("Image", ""),
        "nano_cpus": int(host.get("NanoCpus") or 0),
        "cpu_quota": int(host.get("CpuQuota") or 0),
        "cpu_period": int(host.get("CpuPeriod") or 0),
        "cpu_set": host.get("CpusetCpus") or "",
        "memory_limit_bytes": int(host.get("Memory") or 0),
    }

names = ("api", "worker", "fulfillment", "notification", "postgres")
services = {name: container_info(name) for name in names}
image_id = run(["docker", "image", "inspect", "--format", "{{.Id}}", "webhook-platform:local"]).stdout.strip()
services["benchmark"] = {
    "image_ref": "webhook-platform:local",
    "image_id": image_id,
    "nano_cpus": 0,
    "cpu_quota": 0,
    "cpu_period": 0,
    "cpu_set": "",
    "memory_limit_bytes": 0,
}

worker_id = run(["docker", "compose", "ps", "-q", "worker"]).stdout.strip()
worker = json.loads(run(["docker", "inspect", worker_id]).stdout)[0]
worker_env = {}
for value in worker.get("Config", {}).get("Env", []):
    key, separator, content = value.partition("=")
    if separator and key in {
        "WORKER_CONCURRENCY", "WORKER_POLL_INTERVAL", "LEASE_DURATION",
        "LEASE_MARGIN", "HTTP_TIMEOUT", "DB_STATEMENT_TIMEOUT",
    }:
        worker_env[key] = content
try:
    concurrency = int(worker_env.get("WORKER_CONCURRENCY", "4"))
except ValueError:
    print("Worker concurrency metadata is invalid.", file=sys.stderr)
    raise SystemExit(2)
worker_config = {
    "concurrency": concurrency,
    "poll_interval": worker_env.get("WORKER_POLL_INTERVAL", "250ms"),
    "lease_duration": worker_env.get("LEASE_DURATION", "30s"),
    "lease_margin": worker_env.get("LEASE_MARGIN", "5s"),
    "http_timeout": worker_env.get("HTTP_TIMEOUT", "5s"),
    "db_statement_timeout": worker_env.get("DB_STATEMENT_TIMEOUT", "3s"),
    "database_pool_max": concurrency + 2,
}
print(json.dumps({"services": services, "worker_configuration": worker_config}, separators=(",", ":")))
PY
)

uid=$(id -u)
gid=$(id -g)
docker compose --profile benchmark run --rm --no-deps --user "$uid:$gid" \
	-e "BENCHMARK_HOST_METADATA=$host_metadata" \
	-e "BENCHMARK_RUNTIME_METADATA=$runtime_metadata" \
	benchmark "$@"
