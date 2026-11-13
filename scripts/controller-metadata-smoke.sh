#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

for command in go curl nc python3; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done

CLIENT_PORTS=(29692 29693 29694)
PEER_PORTS=(29792 29793 29794)
ADMIN_PORTS=(29892 29893 29894)
ALL_PORTS=("${CLIENT_PORTS[@]}" "${PEER_PORTS[@]}" "${ADMIN_PORTS[@]}")
for port in "${ALL_PORTS[@]}"; do
  if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
    echo "required test port is already in use: $port" >&2
    exit 1
  fi
done

TEMP_ROOT="$(mktemp -d)"
BROKER_BIN="$TEMP_ROOT/messlessmq-broker"
CLIENT_BIN="$TEMP_ROOT/messlessmq-client"
VOTERS="broker-1,broker-2,broker-3"
ASSIGNMENTS='orders:0=broker-1,broker-2,broker-3;orders:1=broker-2,broker-3,broker-1;orders:2=broker-3,broker-1,broker-2'
PIDS=()

cleanup() {
  local pid
  for pid in "${PIDS[@]}"; do
    if [[ -n "$pid" ]]; then kill -TERM "$pid" >/dev/null 2>&1 || true; fi
  done
  for pid in "${PIDS[@]}"; do
    if [[ -n "$pid" ]]; then wait "$pid" >/dev/null 2>&1 || true; fi
  done
  rm -rf "$TEMP_ROOT"
}
trap cleanup EXIT

echo "Building broker and client binaries..."
go build -o "$BROKER_BIN" ./cmd/broker
go build -o "$CLIENT_BIN" ./cmd/client

start_broker() {
  local index="$1" id="$2" seeds="$3"
  local data_dir="$TEMP_ROOT/$id-data"
  local log_file="$TEMP_ROOT/$id.log"
  env \
    MESSLESSMQ_BROKER_ID="$id" \
    MESSLESSMQ_CLUSTER_ID=messlessmq-controller-metadata-smoke \
    MESSLESSMQ_LISTEN_ADDRESS="127.0.0.1:${CLIENT_PORTS[$index]}" \
    MESSLESSMQ_ADVERTISE_ADDRESS="127.0.0.1:${CLIENT_PORTS[$index]}" \
    MESSLESSMQ_PEER_LISTEN_ADDRESS="127.0.0.1:${PEER_PORTS[$index]}" \
    MESSLESSMQ_PEER_ADVERTISE_ADDRESS="127.0.0.1:${PEER_PORTS[$index]}" \
    MESSLESSMQ_SEEDS="$seeds" \
    MESSLESSMQ_PEER_JOIN_TIMEOUT=15s \
    MESSLESSMQ_CONTROLLER_VOTERS="$VOTERS" \
    MESSLESSMQ_ELECTION_TIMEOUT_MIN=800ms \
    MESSLESSMQ_ELECTION_TIMEOUT_MAX=1400ms \
    MESSLESSMQ_HEARTBEAT_INTERVAL=150ms \
    MESSLESSMQ_REPLICA_ASSIGNMENTS="$ASSIGNMENTS" \
    MESSLESSMQ_MIN_ISR=2 \
    MESSLESSMQ_ADMIN_ADDRESS="127.0.0.1:${ADMIN_PORTS[$index]}" \
    MESSLESSMQ_DATA_DIR="$data_dir" \
    MESSLESSMQ_BOOTSTRAP_TOPICS=orders:3 \
    MESSLESSMQ_LOG_LEVEL=warn \
    "$BROKER_BIN" >"$log_file" 2>&1 &
  PIDS[$index]="$!"
}

wait_for_admin() {
  local port="$1" log_file="$2" attempts=0
  until curl -fsS "http://127.0.0.1:$port/health/live" >/dev/null 2>&1; do
    if (( attempts >= 150 )); then
      echo "admin endpoint on port $port did not become ready" >&2
      cat "$log_file" >&2
      exit 1
    fi
    sleep 0.1
    ((attempts += 1))
  done
}

wait_for_committed_metadata() {
  local attempts=0 port body
  while (( attempts < 200 )); do
    local all_ready=1
    for port in "${ADMIN_PORTS[@]}"; do
      body="$(curl -fsS "http://127.0.0.1:$port/controller/metadata" 2>/dev/null || true)"
      if ! python3 - "$body" <<'PY'
import json, sys
try:
    data=json.loads(sys.argv[1])
except Exception:
    raise SystemExit(1)
brokers=data.get("brokers") or []
partitions=data.get("partitions") or []
ids={broker.get("id") for broker in brokers}
if ids != {"broker-1", "broker-2", "broker-3"} or len(partitions) != 3:
    raise SystemExit(1)
PY
      then
        all_ready=0
        break
      fi
    done
    if (( all_ready == 1 )); then
      echo "All brokers applied three committed registrations and three committed partition assignments."
      return 0
    fi
    sleep 0.1
    ((attempts += 1))
  done
  echo "committed controller metadata did not converge" >&2
  local index
  for index in 0 1 2; do
    echo "--- broker-$((index + 1)) log ---" >&2
    cat "$TEMP_ROOT/broker-$((index + 1)).log" >&2
  done
  exit 1
}

start_broker 0 broker-1 ""
wait_for_admin "${ADMIN_PORTS[0]}" "$TEMP_ROOT/broker-1.log"
start_broker 1 broker-2 "broker-1@127.0.0.1:${PEER_PORTS[0]}"
wait_for_admin "${ADMIN_PORTS[1]}" "$TEMP_ROOT/broker-2.log"
start_broker 2 broker-3 "broker-1@127.0.0.1:${PEER_PORTS[0]},broker-2@127.0.0.1:${PEER_PORTS[1]}"
wait_for_admin "${ADMIN_PORTS[2]}" "$TEMP_ROOT/broker-3.log"
wait_for_committed_metadata

for index in 0 1 2; do
  output="$("$CLIENT_BIN" metadata -addr "127.0.0.1:${CLIENT_PORTS[$index]}" -topic orders)"
  grep -Fq 'metadata_version=' <<<"$output"
  grep -Fq 'broker id=broker-1' <<<"$output"
  grep -Fq 'broker id=broker-2' <<<"$output"
  grep -Fq 'broker id=broker-3' <<<"$output"
  grep -Fq 'partition=0 leader=broker-1 replicas=[broker-1 broker-2 broker-3]' <<<"$output"
  grep -Fq 'partition=1 leader=broker-2 replicas=[broker-2 broker-3 broker-1]' <<<"$output"
  grep -Fq 'partition=2 leader=broker-3 replicas=[broker-3 broker-1 broker-2]' <<<"$output"
done
echo "Client METADATA API exposes the committed broker and partition assignments on all three brokers."

"$CLIENT_BIN" produce -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -key customer-7 -value controller-committed-order -acks all
for index in 1 2; do
  output="$("$CLIENT_BIN" consume -addr "127.0.0.1:${CLIENT_PORTS[$index]}" -topic orders -partition 0 -offset 0 -max 10)"
  grep -Fq 'key="customer-7"' <<<"$output"
  grep -Fq 'value="controller-committed-order"' <<<"$output"
  echo "broker-$((index + 1)) consumed the record from its own replica log."
done

echo "Three-process committed controller metadata and data-plane integration smoke test passed."
