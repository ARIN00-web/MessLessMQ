#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v nc >/dev/null 2>&1; then
  echo "nc is required for the TCP readiness check." >&2
  exit 1
fi

BROKER_HOST="127.0.0.1"
BROKER_PORT="19092"
BROKER_ADDR="${BROKER_HOST}:${BROKER_PORT}"
if nc -z "$BROKER_HOST" "$BROKER_PORT" >/dev/null 2>&1; then
  echo "Port ${BROKER_PORT} is already in use; stop that process before running this test." >&2
  exit 1
fi

WORK_DIR="$(mktemp -d)"
BROKER_PID=""
cleanup() {
  if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" >/dev/null 2>&1; then
    kill -TERM "$BROKER_PID" >/dev/null 2>&1 || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

go build -o "$WORK_DIR/messlessmq-broker" ./cmd/broker
go build -o "$WORK_DIR/messlessmq-client" ./cmd/client

wait_for_listener() {
  for _ in $(seq 1 100); do
    if [[ -n "$BROKER_PID" ]] && ! kill -0 "$BROKER_PID" >/dev/null 2>&1; then
      echo "Broker exited before becoming ready. Log follows:" >&2
      cat "$WORK_DIR/broker.log" >&2
      return 1
    fi
    if nc -z "$BROKER_HOST" "$BROKER_PORT" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "Timed out waiting for broker listener. Log follows:" >&2
  cat "$WORK_DIR/broker.log" >&2
  return 1
}

start_broker() {
  MESSLESSMQ_BROKER_ID=crash-test \
  MESSLESSMQ_LISTEN_ADDRESS="$BROKER_ADDR" \
  MESSLESSMQ_ADVERTISE_ADDRESS="$BROKER_ADDR" \
  MESSLESSMQ_DATA_DIR="$WORK_DIR/data" \
  MESSLESSMQ_BOOTSTRAP_TOPICS=orders:1 \
  "$WORK_DIR/messlessmq-broker" >"$WORK_DIR/broker.log" 2>&1 &
  BROKER_PID=$!
  wait_for_listener
}

start_broker
printf "Broker started (pid=%s). Producing a sync-acknowledged record...\n" "$BROKER_PID"
"$WORK_DIR/messlessmq-client" produce -addr "$BROKER_ADDR" -topic orders -key crash-test-key -value persisted-before-crash -acks leader

kill -KILL "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""
sleep 0.2

printf "Simulated abrupt process crash. Restarting against the same data directory...\n"
start_broker
consume_output="$("$WORK_DIR/messlessmq-client" consume -addr "$BROKER_ADDR" -topic orders -partition 0 -offset 0 -max 10)"
printf "%s\n" "$consume_output"
if ! grep -Fq 'persisted-before-crash' <<<"$consume_output"; then
  echo "Persisted record was not recovered after restart." >&2
  cat "$WORK_DIR/broker.log" >&2
  exit 1
fi

kill -TERM "$BROKER_PID"
wait "$BROKER_PID"
BROKER_PID=""
printf "PASS: sync-acknowledged single-broker record survived SIGKILL/restart; SIGTERM shutdown completed. This does not prove replica recovery.\n"
