#!/bin/sh

set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"

if [ ! -f .env ]; then
	printf '%s\n' 'Missing .env; run make init-env first.' >&2
	exit 2
fi
if ! docker compose version >/dev/null 2>&1; then
	printf '%s\n' 'Docker Compose v2 is required.' >&2
	exit 2
fi

if docker compose up -d --build --wait --wait-timeout 120; then
	printf '%s\n' 'Compose services started and healthy.'
	exit 0
fi

python3 - <<'PY'
import json
import subprocess
import sys

result = subprocess.run(
    ["docker", "compose", "ps", "--all", "--format", "json"],
    text=True,
    capture_output=True,
)
if result.returncode == 0:
    try:
        rows = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    except json.JSONDecodeError:
        rows = []
    for row in rows:
        name = row.get("Service") or row.get("Name") or "service"
        state = row.get("State") or "unknown"
        health = row.get("Health") or ""
        print("{}: {} {}".format(name, state, health).strip(), file=sys.stderr)
print("Compose did not become healthy within 120 seconds; no volumes were removed.", file=sys.stderr)
PY
exit 1
