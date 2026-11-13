#!/bin/sh

set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"

if [ ! -f .env ]; then
	printf '%s\n' 'Missing .env; run make init-env, then make verify.' >&2
	exit 2
fi

# Values are loaded into this process only. This script never prints .env or
# resolved Compose configuration.
set -a
. ./.env
set +a

if [ -z "${TEST_DATABASE_ADMIN_URL:-}" ]; then
	printf '%s\n' 'TEST_DATABASE_ADMIN_URL is required for isolated PostgreSQL integration tests.' >&2
	exit 2
fi

scratch_dir=$(mktemp -d "${TMPDIR:-/tmp}/webhook-verify.XXXXXX")
project_name="webhook-verify-$(python3 -c 'import secrets; print(secrets.token_hex(6))')"

cleanup() {
	docker compose down --timeout 15 >/dev/null 2>&1 || true
	rm -rf "$scratch_dir"
}
trap cleanup EXIT HUP INT TERM

set -- $(python3 - <<'PY'
import socket

sockets = []
try:
    ports = []
    for _ in range(2):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
        ports.append(sock.getsockname()[1])
    if ports[0] == ports[1]:
        raise SystemExit("failed to reserve distinct local port numbers")
    print(*ports)
finally:
    for sock in sockets:
        sock.close()
PY
)
if [ "$#" -ne 2 ]; then
	printf '%s\n' 'Could not select isolated API and PostgreSQL host ports.' >&2
	exit 1
fi
export API_HOST_PORT=$1
export PG_HOST_PORT=$2
export COMPOSE_PROJECT_NAME=$project_name
export COMPOSE_SMOKE_MARKER="$scratch_dir/compose-smoke-event.json"

# Keep the configured credentials and maintenance database, but direct the
# tests to this run's newly created Compose PostgreSQL instance.
export TEST_DATABASE_ADMIN_URL=$(python3 - "$PG_HOST_PORT" <<'PY'
import os
import sys
from urllib.parse import urlsplit, urlunsplit

try:
    parsed = urlsplit(os.environ["TEST_DATABASE_ADMIN_URL"])
    if parsed.scheme not in ("postgres", "postgresql") or parsed.path != "/postgres":
        raise ValueError
    userinfo = parsed.netloc.rsplit("@", 1)[0] + "@" if "@" in parsed.netloc else ""
    print(urlunsplit((parsed.scheme, userinfo + "127.0.0.1:" + sys.argv[1], parsed.path, parsed.query, parsed.fragment)))
except (KeyError, ValueError):
    print("TEST_DATABASE_ADMIN_URL must point to the PostgreSQL postgres maintenance database.", file=sys.stderr)
    raise SystemExit(2)
PY
)

printf '%s\n' 'Running clean-room release verification in an isolated Compose project.'
make doctor

formatting=$(find . \
	-type d \( -name .git -o -name node_modules -o -name bin \) -prune -o \
	-type f -name '*.go' -exec gofmt -l {} +)
if [ -n "$formatting" ]; then
	printf '%s\n' 'Go formatting check failed:' "$formatting" >&2
	exit 1
fi
printf '%s\n' 'Go formatting check passed.'

make vet
make test
make test-race
make build

cd web
npm ci --no-audit --no-fund
cd "$root_dir"
make web-check

# A unique project name gives this run a newly created named volume and database.
make up
export GOFLAGS=-count=1
make test-integration
make test-crash
unset GOFLAGS

make demo
make compose-smoke
make down
make up
make compose-smoke

python3 ./scripts/release-secret-scan.py
git diff --check
git diff --cached --check

printf 'Release verification passed. Compose project: %s; API port: %s; PostgreSQL port: %s.\n' \
	"$COMPOSE_PROJECT_NAME" "$API_HOST_PORT" "$PG_HOST_PORT"
printf '%s\n' 'The verification stack is stopped by cleanup; its PostgreSQL volume is retained.'
