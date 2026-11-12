#!/bin/sh

set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"

if [ ! -f .env ]; then
	printf '%s\n' 'Missing .env; initialize local configuration before running this smoke.' >&2
	exit 2
fi

set -a
. ./.env
set +a
: "${DATABASE_URL:?DATABASE_URL must be configured in .env}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be configured in .env}"

if python3 -c 'import socket, sys; s = socket.socket(); s.settimeout(0.25); result = s.connect_ex(("127.0.0.1", 8080)); s.close(); sys.exit(0 if result == 0 else 1)'; then
	printf '%s\n' 'Port 8080 is already in use; stop the local API before running this smoke.' >&2
	exit 2
fi

suffix=$(python3 -c 'import secrets; print(secrets.token_hex(6))')
test_database="webhook_test_task22_${suffix}"
task_token=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
test_database_url=$(python3 - "$test_database" <<'PY'
import os
import sys
from urllib.parse import urlsplit, urlunsplit

parts = urlsplit(os.environ["DATABASE_URL"])
print(urlunsplit(parts._replace(path="/" + sys.argv[1])))
PY
)
temporary_dir=$(mktemp -d)
auth_config="$temporary_dir/curl-auth.conf"
api_pid=
created_database=0
umask 077
printf 'header = "Authorization: Bearer %s"\n' "$task_token" > "$auth_config"

cleanup() {
	if [ -n "$api_pid" ]; then
		kill "$api_pid" 2>/dev/null || true
		wait "$api_pid" 2>/dev/null || true
	fi
	if [ "$created_database" -eq 1 ]; then
		docker compose exec -e PGOPTIONS='-c statement_timeout=5000' -T postgres psql -U webhook -d postgres -v ON_ERROR_STOP=1 \
			-c "DROP DATABASE IF EXISTS $test_database" >/dev/null 2>&1 || true
	fi
	rm -rf "$temporary_dir"
}
trap cleanup EXIT

docker compose exec -e PGOPTIONS='-c statement_timeout=5000' -T postgres psql -U webhook -d postgres -v ON_ERROR_STOP=1 \
	-c "CREATE DATABASE $test_database" >/dev/null
created_database=1

RUN_MODE=host go run ./cmd/migrate --database "$test_database" >/dev/null
RUN_MODE=host DATABASE_URL="$test_database_url" go run ./cmd/seed >/dev/null
go build -o "$temporary_dir/api" ./cmd/api
RUN_MODE=host DATABASE_URL="$test_database_url" API_TOKEN="$task_token" \
	"$temporary_dir/api" >"$temporary_dir/api.log" 2>&1 &
api_pid=$!

ready=0
attempt=0
while [ "$attempt" -lt 30 ]; do
	if curl --silent --fail --max-time 1 http://127.0.0.1:8080/health/live >/dev/null 2>&1; then
		ready=1
		break
	fi
	attempt=$((attempt + 1))
	sleep 0.25
done
if [ "$ready" -ne 1 ]; then
	printf '%s\n' 'The isolated API did not become ready.' >&2
	exit 1
fi

destinations=$(curl --silent --show-error --fail --max-time 10 --config "$auth_config" \
	http://127.0.0.1:8080/v1/destinations)
destination_id=$(DESTINATIONS_JSON="$destinations" python3 -c '
import json, os
items = json.loads(os.environ["DESTINATIONS_JSON"])["items"]
print(next(item["id"] for item in items if item["code"] == "fulfillment"))
')
order_id="task22-smoke-$suffix"
event_body=$(DESTINATION_ID="$destination_id" ORDER_ID="$order_id" python3 -c '
import json, os
print(json.dumps({
    "type": "order.confirmed",
    "payload": {"scenario_id": "task22-smoke", "order_id": os.environ["ORDER_ID"], "quantity": 1},
    "destination_ids": [os.environ["DESTINATION_ID"]],
}, separators=(",", ":")))
')
create_key="task22-create-$suffix"
created_event=$(curl --silent --show-error --fail --max-time 10 --config "$auth_config" \
	-H 'Content-Type: application/json' -H "Idempotency-Key: $create_key" \
	--data-binary "$event_body" http://127.0.0.1:8080/v1/events)
identifiers=$(CREATE_RESPONSE="$created_event" python3 -c '
import json, os
import uuid
result = json.loads(os.environ["CREATE_RESPONSE"])
print(uuid.UUID(result["event_id"]), uuid.UUID(result["delivery_ids"][0]))
')
event_id=${identifiers%% *}
delivery_id=${identifiers#* }

updated_delivery=$(docker compose exec -e PGOPTIONS='-c statement_timeout=5000' -T postgres psql -U webhook -d "$test_database" -qAt \
	-c "UPDATE deliveries SET status='dead', terminal_reason='attempts_exhausted', updated_at=CURRENT_TIMESTAMP WHERE id='$delivery_id'::uuid RETURNING id")
if [ "$updated_delivery" != "$delivery_id" ]; then
	printf '%s\n' 'Could not prepare the isolated dead-delivery fixture.' >&2
	exit 1
fi

replay_key="task22-replay-$suffix"
replay_result=$(curl --silent --show-error --fail --max-time 10 --config "$auth_config" \
	-H 'Content-Type: application/json' -H "Idempotency-Key: $replay_key" \
	--data-binary '{}' "http://127.0.0.1:8080/v1/deliveries/$delivery_id/replay")
detail=$(curl --silent --show-error --fail --max-time 10 --config "$auth_config" \
	"http://127.0.0.1:8080/v1/events/$event_id")
EVENT_ID="$event_id" DELIVERY_ID="$delivery_id" REPLAY_RESPONSE="$replay_result" DETAIL_RESPONSE="$detail" \
	python3 -c '
import json, os
replay = json.loads(os.environ["REPLAY_RESPONSE"])
detail = json.loads(os.environ["DETAIL_RESPONSE"])
delivery = next(item for item in detail["deliveries"] if item["id"] == os.environ["DELIVERY_ID"])
assert detail["id"] == os.environ["EVENT_ID"]
assert replay["delivery_id"] == delivery["id"] and replay["cycle_no"] == 2
assert delivery["status"] == "pending" and delivery["cycle_no"] == 2
assert len(delivery["replays"]) == 1
print("real API create and dead-delivery replay smoke: PASS (accepted event; replay scheduled; cycle 2 pending)")
'
