#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== Full test suite =="
go test -count=1 ./...

echo "== Race detector =="
go test -race -count=1 ./...

echo "== Build broker and client =="
go build ./cmd/broker ./cmd/client

echo "== Static analysis =="
go vet ./...

echo "All validation commands completed successfully."
