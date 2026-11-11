#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

go test -count=1 ./internal/server ./internal/network ./internal/broker ./internal/protocol ./internal/cluster ./internal/interbroker ./internal/membership ./internal/config ./internal/consumer ./internal/group ./internal/offsets
