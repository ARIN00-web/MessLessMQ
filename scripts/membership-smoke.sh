#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

for command in go curl nc; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done

CLIENT_PORTS=(29092 29093 29094)
PEER_PORTS=(29192 29193 29194)
ADMIN_PORTS=(29292 29293 29294)
ALL_PORTS=("${CLIENT_PORTS[@]}" "${PEER_PORTS[@]}" "${ADMIN_PORTS[@]}")
for port in "${ALL_PORTS[@]}"; do
  if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
    echo "required test port is already in use: $port" >&2
    exit 1
  fi
done

TEMP_ROOT="$(mktemp -d)"
BROKER_BIN="$TEMP_ROOT/messlessmq-broker"
PIDS=()

cleanup() {
  local pid
  for pid in "${PIDS[@]}"; do
    kill -TERM "$pid" >/dev/null 2>&1 || true
  done
  for pid in "${PIDS[@]}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
  rm -rf "$TEMP_ROOT"
}
trap cleanup EXIT

 echo "Building broker binary..."
go build -o "$BROKER_BIN" ./cmd/broker

start_broker() {
  local id="$1"
  local client_port="$2"
  local peer_port="$3"
  local admin_port="$4"
  local seed_list="$5"
  local data_dir="$TEMP_ROOT/$id"

  env \
    MESSLESSMQ_BROKER_ID="$id" \
    MESSLESSMQ_CLUSTER_ID=messlessmq-membership-smoke \
    MESSLESSMQ_LISTEN_ADDRESS="127.0.0.1:$client_port" \
    MESSLESSMQ_ADVERTISE_ADDRESS="127.0.0.1:$client_port" \
    MESSLESSMQ_PEER_LISTEN_ADDRESS="127.0.0.1:$peer_port" \
    MESSLESSMQ_PEER_ADVERTISE_ADDRESS="127.0.0.1:$peer_port" \
    MESSLESSMQ_SEEDS="$seed_list" \
    MESSLESSMQ_PEER_JOIN_TIMEOUT=15s \
    MESSLESSMQ_ADMIN_ADDRESS="127.0.0.1:$admin_port" \
    MESSLESSMQ_DATA_DIR="$data_dir" \
    MESSLESSMQ_BOOTSTRAP_TOPICS=orders:1 \
    MESSLESSMQ_LOG_LEVEL=warn \
    "$BROKER_BIN" >"$TEMP_ROOT/$id.log" 2>&1 &
  PIDS+=("$!")
}

wait_for_admin() {
  local port="$1"
  local log_file="$2"
  local attempts=0
  until curl -fsS "http://127.0.0.1:$port/health/live" >/dev/null 2>&1; do
    if (( attempts >= 100 )); then
      echo "broker admin endpoint on port $port did not become ready" >&2
      cat "$log_file" >&2
      exit 1
    fi
    sleep 0.1
    ((attempts += 1))
  done
}

wait_for_member_count() {
  local admin_port="$1"
  local log_file="$2"
  local attempts=0
  local snapshot
  until snapshot="$(curl -fsS "http://127.0.0.1:$admin_port/cluster/members" 2>/dev/null)" && \
    grep -Fq '"id":"broker-1"' <<<"$snapshot" && \
    grep -Fq '"id":"broker-2"' <<<"$snapshot" && \
    grep -Fq '"id":"broker-3"' <<<"$snapshot"; do
    if (( attempts >= 100 )); then
      echo "broker membership on admin port $admin_port did not converge to all three members" >&2
      cat "$log_file" >&2
      echo "last snapshot: ${snapshot:-<unavailable>}" >&2
      exit 1
    fi
    sleep 0.1
    ((attempts += 1))
  done
  echo "admin port $admin_port discovered all three brokers"
}

start_broker broker-1 "${CLIENT_PORTS[0]}" "${PEER_PORTS[0]}" "${ADMIN_PORTS[0]}" ""
wait_for_admin "${ADMIN_PORTS[0]}" "$TEMP_ROOT/broker-1.log"

start_broker broker-2 "${CLIENT_PORTS[1]}" "${PEER_PORTS[1]}" "${ADMIN_PORTS[1]}" "broker-1@127.0.0.1:${PEER_PORTS[0]}"
wait_for_admin "${ADMIN_PORTS[1]}" "$TEMP_ROOT/broker-2.log"

start_broker broker-3 "${CLIENT_PORTS[2]}" "${PEER_PORTS[2]}" "${ADMIN_PORTS[2]}" "broker-1@127.0.0.1:${PEER_PORTS[0]},broker-2@127.0.0.1:${PEER_PORTS[1]}"
wait_for_admin "${ADMIN_PORTS[2]}" "$TEMP_ROOT/broker-3.log"

for index in 0 1 2; do
  wait_for_member_count "${ADMIN_PORTS[$index]}" "$TEMP_ROOT/broker-$((index + 1)).log"
done

echo "Three-process broker membership smoke test passed."
