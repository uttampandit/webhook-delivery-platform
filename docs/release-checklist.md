# Local release checklist

**Status:** Local release gates passed 2026-11-12. Hosting is separate and not requested. Candidate understanding remains unassessed.

This checklist records evidence for the local, synthetic project only. Hosting has a separate status in [the deployment plan](deployment-plan.md). The release command creates a fresh Compose project and PostgreSQL volume, runs the actual PostgreSQL integration and crash suites, and then checks a demo and a second smoke run after `make down` / `make up`. It keeps that verification volume when the containers stop.

## Run record

| Field | Value |
|---|---|
| Date | 2026-11-12 |
| Verified source snapshot | Fresh clone based on Task 27 draft `52529f4e4760372f2ab1d7a61f69c6650239a075` with the staged Task 28 source changes; Task 28 draft commit is recorded in local `state/EVIDENCE.md` |
| Checkout | Fresh local clone; private `.env` generated there with `make init-env` |
| Command | `make verify` |
| Environment | macOS 27.2 arm64; Go 1.27.3; Node 26.13.0; npm 11.20.0; Docker Engine 29.9.2; Compose 5.6.1; PostgreSQL 17.11 |
| Isolated Compose project | `webhook-verify-c184219fcf7c`; API loopback port 61168; PostgreSQL loopback port 61169; fresh project-scoped volume |
| Demo report | [Five passed scenarios](../artifacts/demo/demo_20261112T115454Z_545b754d33f9/report.json) |
| Retained-volume smoke | Passed after `make down` / `make up`; the second smoke found the earlier accepted event and both receiver effects |

## Release gates

| Gate | Evidence and result |
|---|---|
| Go formatting, vet, unit tests, and race tests | Passed in `make verify` |
| Go binaries and runtime image build | Passed in `make verify` |
| Clean frontend install, lint, typecheck, tests, and production build | Passed; 8 files / 25 tests; `npm ci` emitted a nonfatal package install-script notice for optional `fsevents` |
| Real PostgreSQL integration and SIGKILL recovery tests | Passed against the fresh Compose PostgreSQL instance; crash suite completed in 17.040s; test DSN was not recorded |
| Five synthetic demo stories and receiver counts | Passed; see the linked sanitized report |
| Image user, runtime-secret separation, UI bundle, and host port exposure | Passed; image runs as UID 10001; only API/PostgreSQL published on loopback; receiver, control, and metrics ports private |
| Repository, runtime image files, frontend asset, and Compose logs secret review | Passed; scanner emitted no secret values |
| Event/effect retention across Compose restart | Passed; second smoke run retained the earlier event and one committed effect per receiver |
| Documentation, relative links, and diff review | Passed; local claims match implementation and evidence; link check and `git diff --check` passed |
| Candidate understanding of acceptance vs. external effect and fencing limits | **Not assessed**; exercises are available, but test results cannot establish personal understanding |

## Review boundary

The release command created its fresh Compose stack, ran `make test-integration` and `make test-crash` with `GOFLAGS=-count=1`, ran the five-story demo, performed Compose smoke, stopped the project with `make down`, started it again, and repeated Compose smoke. Its isolated project was stopped during cleanup, but the unique volume remains. The test DSN was generated from the temporary `.env`, redirected to the fresh PostgreSQL port, and never printed or recorded.

The repository, the built image filesystem, the served frontend bundle, and the active Compose logs were checked against values in the local `.env`; the scan never prints those values. Compose checks the resolved and running port bindings: only API and PostgreSQL are published, both on loopback, while receiver control, metrics, and receiver ports remain private. These checks describe the tested local configuration; they do not prove host security outside this Compose project.

The release review also checks that README, demo, benchmark, and interview statements match the implementation and linked evidence. The finite benchmark reports remain workload measurements, not capacity or production-readiness claims. An unanswered learning exercise remains candidate-owned evidence.
