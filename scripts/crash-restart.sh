#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v nc >/dev/null 2>&1; then
  echo "nc is required for the TCP readiness check." >&2
  exit 1
fi

if nc -z 127.0.0.1 9092 >/dev/null 2>&1; then
  echo "Port 9092 is already in use; stop that process before running this test." >&2
  exit 1
fi

WORK_DIR="$(mktemp -d)"
BROKER_PID=""
cleanup() {
  if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" >/dev/null 2>&1; then
    kill "$BROKER_PID" >/dev/null 2>&1 || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

go build -o "$WORK_DIR/messlessmq-broker" ./cmd/broker

wait_for_listener() {
  for _ in $(seq 1 100); do
    if [[ -n "$BROKER_PID" ]] && ! kill -0 "$BROKER_PID" >/dev/null 2>&1; then
      return 1
    fi
    if nc -z 127.0.0.1 9092 >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

start_broker() {
  "$WORK_DIR/messlessmq-broker" >"$WORK_DIR/broker.log" 2>&1 &
  BROKER_PID=$!
  wait_for_listener
}

start_broker
echo "Broker started and accepted TCP connections (pid=$BROKER_PID)."
kill -KILL "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""
sleep 0.2

echo "Simulated abrupt process crash. Restarting..."
start_broker
echo "Broker restarted and accepted TCP connections (pid=$BROKER_PID)."
kill -TERM "$BROKER_PID"
wait "$BROKER_PID"
BROKER_PID=""
echo "Graceful signal shutdown completed."
echo "PASS: process restart smoke test. This does not prove message durability or replica recovery."
