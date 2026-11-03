GO ?= go
NPM ?= npm

.PHONY: doctor build test test-race test-integration vet web-check init-env seed up down

doctor:
	@sh ./scripts/doctor.sh

build:
	mkdir -p bin
	$(GO) build -o bin/ ./...

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

vet:
	$(GO) vet ./...

web-check:
	cd web && $(NPM) run typecheck && $(NPM) run build

init-env:
	sh ./scripts/init-env.sh

seed:
	@if [ ! -f .env ]; then \
		printf '%s\n' 'Missing .env; run make init-env first.' >&2; \
		exit 2; \
	fi
	@set -a; . ./.env; set +a; $(GO) run ./cmd/seed

up:
	docker compose up -d postgres

down:
	docker compose down
