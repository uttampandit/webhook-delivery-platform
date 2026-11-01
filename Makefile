GO ?= go
NPM ?= npm

.PHONY: doctor build test test-race vet web-check

doctor:
	@sh ./scripts/doctor.sh

build:
	mkdir -p bin
	$(GO) build -o bin/ ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

web-check:
	cd web && $(NPM) run typecheck && $(NPM) run build
