#!/usr/bin/env bash

set -euo pipefail

root_dir=$(cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"

recording_id=$(date -u +%Y%m%dT%H%M%SZ)
recording_dir="artifacts/demo/recording-$recording_id-$$"
mkdir -p "$recording_dir"
chmod 700 "$recording_dir"

{
	printf '%s\n' '$ make demo'
	make demo
} 2>&1 | tee "$recording_dir/transcript.txt"

printf 'Unedited command transcript: %s/transcript.txt\n' "$recording_dir"
