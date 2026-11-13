GO ?= go
NPM ?= npm

.PHONY: doctor build test test-race test-integration test-crash vet web-check web-api-smoke compose-smoke demo demo-recording benchmark init-env seed up down

BENCHMARK_EVENTS ?= 200
BENCHMARK_PRODUCER_CONCURRENCY ?= 4
BENCHMARK_PAYLOAD_BYTES ?= 1024
BENCHMARK_TIMEOUT ?= 180s
BENCHMARK_RUN_ID ?=

doctor:
	@sh ./scripts/doctor.sh

build:
	mkdir -p bin
	$(GO) build -o bin/ ./...
	docker build -t webhook-platform:local .

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

test-integration:
	@if [ -z "$${TEST_DATABASE_ADMIN_URL:-}" ]; then \
		printf '%s\n' 'TEST_DATABASE_ADMIN_URL is required; point it at the PostgreSQL postgres maintenance database with CREATEDB and DROP privileges.' >&2; \
		exit 2; \
	fi
	$(GO) test -tags integration ./...

test-crash:
	@if [ -z "$${TEST_DATABASE_ADMIN_URL:-}" ]; then \
		printf '%s\n' 'TEST_DATABASE_ADMIN_URL is required; point it at the PostgreSQL postgres maintenance database with CREATEDB and DROP privileges.' >&2; \
		exit 2; \
	fi
	$(GO) test -timeout 240s -tags 'integration,crash' ./tests/crash

vet:
	$(GO) vet ./...

web-check:
	cd web && $(NPM) run lint && $(NPM) run typecheck && $(NPM) test -- --run && $(NPM) run build

web-api-smoke:
	sh ./scripts/web-api-smoke.sh

compose-smoke:
	python3 ./scripts/compose-smoke.py

demo:
	sh ./scripts/demo.sh

demo-recording:
	bash ./scripts/demo-recording.sh

benchmark:
	sh ./scripts/benchmark.sh \
		--events="$(BENCHMARK_EVENTS)" \
		--producer-concurrency="$(BENCHMARK_PRODUCER_CONCURRENCY)" \
		--payload-bytes="$(BENCHMARK_PAYLOAD_BYTES)" \
		--timeout="$(BENCHMARK_TIMEOUT)" \
		$(if $(strip $(BENCHMARK_RUN_ID)),--run-id="$(BENCHMARK_RUN_ID)",)

init-env:
	sh ./scripts/init-env.sh

seed:
	@if [ ! -f .env ]; then \
		printf '%s\n' 'Missing .env; run make init-env first.' >&2; \
		exit 2; \
	fi
	@set -a; . ./.env; set +a; $(GO) run ./cmd/seed

up:
	sh ./scripts/compose-up.sh

down:
	docker compose down --timeout 15
