#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COUNT="${COUNT:-3}"
BENCHTIME="${BENCHTIME:-2s}"
go test -run '^$' -bench . -benchmem -benchtime="$BENCHTIME" -count="$COUNT" ./internal/performance
