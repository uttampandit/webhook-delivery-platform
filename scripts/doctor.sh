#!/bin/sh

set -u

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"

failures=0

check_exact() {
	name=$1
	command_name=$2
	version_args=$3
	expected_file=$4
	version_pattern=$5

	if ! command -v "$command_name" >/dev/null 2>&1; then
		printf 'MISSING %s: install the version pinned in %s\n' "$name" "$expected_file"
		failures=$((failures + 1))
		return
	fi

	expected=$(cat "$expected_file")
	if [ "$name" = "Node.js" ]; then
		expected=${expected#v}
	fi
	actual=$("$command_name" $version_args 2>&1 | sed -n "$version_pattern")
	if [ -z "$actual" ]; then
		printf 'ERROR %s: could not read version; expected %s from %s\n' "$name" "$expected" "$expected_file"
		failures=$((failures + 1))
	elif [ "$actual" = "$expected" ]; then
		printf 'OK %s %s\n' "$name" "$actual"
	else
		printf 'ERROR %s: found %s; expected %s from %s\n' "$name" "$actual" "$expected" "$expected_file"
		failures=$((failures + 1))
	fi
}

check_exact "Go" go "version" .go-version 's/.*go\([0-9][^ ]*\).*/\1/p'
check_exact "Node.js" node "--version" .nvmrc 's/^v//p'
check_exact "npm" npm "--version" .npm-version 'p'

if command -v docker >/dev/null 2>&1; then
	printf 'INFO Docker CLI: %s\n' "$(docker --version 2>&1)"
	if docker compose version >/dev/null 2>&1; then
		printf 'INFO Docker Compose: %s\n' "$(docker compose version --short 2>/dev/null || docker compose version 2>&1)"
	else
		printf 'WARNING Docker Compose plugin is unavailable; PostgreSQL integration checks need it\n'
	fi
	if docker info >/dev/null 2>&1; then
		printf 'OK Docker engine is running\n'
	else
		printf 'WARNING Docker engine is not running; PostgreSQL integration checks are blocked\n'
	fi
else
	printf 'WARNING Docker is unavailable; PostgreSQL integration checks are blocked\n'
fi

if [ "$failures" -ne 0 ]; then
	printf 'Doctor found %s required tool issue(s).\n' "$failures"
	exit 1
fi

printf 'Doctor checks passed for the scaffold toolchain.\n'
