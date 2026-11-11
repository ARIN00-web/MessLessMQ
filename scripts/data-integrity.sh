#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

go test -count=1 ./internal/storage ./internal/replication ./internal/recovery ./internal/offsets ./internal/delivery ./internal/recordmeta
