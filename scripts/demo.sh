#!/bin/sh

set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"

scratch_dir=$(mktemp -d "${TMPDIR:-/tmp}/webhook-demo.XXXXXX")
trap 'rm -rf "$scratch_dir"' EXIT HUP INT TERM

set +e
docker compose exec -T api /app/democtl run >"$scratch_dir/report.json" 2>"$scratch_dir/stderr.txt"
run_status=$?
set -e

if [ -s "$scratch_dir/report.json" ]; then
	python3 - "$scratch_dir/report.json" artifacts/demo "$run_status" <<'PY'
import json
import os
import pathlib
import re
import sys

source = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2])
exit_status = int(sys.argv[3])
raw = source.read_bytes()
if len(raw) > 1_048_576:
    raise SystemExit("demo report exceeded the 1 MiB output limit")
try:
    report = json.loads(raw)
except (UnicodeDecodeError, json.JSONDecodeError):
    raise SystemExit("democtl did not return a JSON report")
run_id = report.get("run_id") if isinstance(report, dict) else None
if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", run_id):
    raise SystemExit("demo report has an invalid run identifier")
if report.get("status") not in ("passed", "failed"):
    raise SystemExit("demo report has an invalid status")
scenarios = report.get("scenarios")
all_passed = isinstance(scenarios, list) and len(scenarios) == 5 and all(
    isinstance(scenario, dict) and scenario.get("status") == "passed"
    for scenario in scenarios
)
if exit_status == 0 and (report["status"] != "passed" or not all_passed):
    raise SystemExit("demo exited successfully without five passed scenarios")
if exit_status != 0 and report["status"] != "failed":
    raise SystemExit("demo failed without a failed report")
root.mkdir(parents=True, exist_ok=True)
destination = root / run_id
destination.mkdir(mode=0o700)
report_path = destination / "report.json"
with report_path.open("xb") as output:
    output.write(raw)
    output.flush()
    os.fsync(output.fileno())
print("Saved sanitized demo report: {}".format(report_path))
PY
	cat "$scratch_dir/report.json"
fi

if [ -s "$scratch_dir/stderr.txt" ]; then
	cat "$scratch_dir/stderr.txt" >&2
fi
exit "$run_status"
