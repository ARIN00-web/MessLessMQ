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

CLIENT_PORTS=(30692 30693 30694)
PEER_PORTS=(30792 30793 30794)
ADMIN_PORTS=(30892 30893 30894)
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
    MESSLESSMQ_CLUSTER_ID=messlessmq-reassignment-smoke \
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
  until curl --connect-timeout 1 --max-time 3 -fsS "http://127.0.0.1:$port/health/live" >/dev/null 2>&1; do
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
      body="$(curl --connect-timeout 1 --max-time 3 -fsS "http://127.0.0.1:$port/controller/metadata" 2>/dev/null || true)"
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

controller_admin() {
  local index status
  for index in 0 1 2; do
    status="$(curl --connect-timeout 1 --max-time 3 -fsS "http://127.0.0.1:${ADMIN_PORTS[$index]}/controller/status" 2>/dev/null || true)"
    if python3 - "$status" <<'PY2'
import json,sys
try: data=json.loads(sys.argv[1])
except Exception: raise SystemExit(1)
raise SystemExit(0 if data.get("role") == "leader" else 1)
PY2
    then
      printf '%s' "http://127.0.0.1:${ADMIN_PORTS[$index]}"
      return 0
    fi
  done
  return 1
}

wait_assignment() {
  local expected="$1" attempts=0 admin
  while (( attempts < 200 )); do
    admin="$(controller_admin || true)"
    if [[ -n "$admin" ]]; then
      body="$(curl --connect-timeout 1 --max-time 3 -fsS "$admin/controller/metadata" 2>/dev/null || true)"
      if python3 - "$body" "$expected" <<'PY2'
import json,sys
try: data=json.loads(sys.argv[1])
except Exception: raise SystemExit(1)
p=next((p for p in data.get("partitions",[]) if p.get("key",{}).get("topic")=="orders" and p.get("key",{}).get("partition")==0),None)
if not p: raise SystemExit(1)
expected=sys.argv[2].split(",")
if p.get("replicas") != expected or p.get("reassignment_target"):
    raise SystemExit(1)
if not set(expected).issubset(set(p.get("isr") or [])):
    raise SystemExit(1)
PY2
      then return 0; fi
    fi
    sleep 0.1
    ((attempts += 1))
  done
  return 1
}

stage_reassignment() {
  local targets="$1" admin payload
  admin="$(controller_admin)"
  payload="$(python3 - "$targets" <<'PY2'
import json,sys
print(json.dumps({"topic":"orders","partition_id":0,"replicas":sys.argv[1].split(",")}))
PY2
)"
  curl --connect-timeout 1 --max-time 10 -fsS -X POST -H 'Content-Type: application/json' -d "$payload" "$admin/controller/reassignments/stage" >/dev/null
}

finalize_reassignment() {
  local admin
  admin="$(controller_admin)"
  curl --connect-timeout 1 --max-time 10 -fsS -X POST -H 'Content-Type: application/json' -d '{"topic":"orders","partition_id":0}' "$admin/controller/reassignments/finalize" >/dev/null
}

# First remove broker-3 only after the controller has staged the target and
# verified its current ISR. Then add it back as a new target; background catch-up
# and ISR reconciliation must make it safe before finalization succeeds.
stage_reassignment 'broker-1,broker-2'
finalize_reassignment
wait_assignment 'broker-1,broker-2'
echo "Controller committed replica removal of broker-3."

"$CLIENT_BIN" produce -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -key customer-7 -value reassignment-catchup-record -acks all
stage_reassignment 'broker-1,broker-2,broker-3'

attempts=0
while (( attempts < 300 )); do
  admin="$(controller_admin || true)"
  body="$(curl --connect-timeout 1 --max-time 3 -fsS "$admin/controller/metadata" 2>/dev/null || true)"
  if python3 - "$body" <<'PY2'
import json,sys
try: data=json.loads(sys.argv[1])
except Exception: raise SystemExit(1)
p=next((p for p in data.get("partitions",[]) if p.get("key",{}).get("topic")=="orders" and p.get("key",{}).get("partition")==0),None)
if not p or not {"broker-1","broker-2","broker-3"}.issubset(set(p.get("isr") or [])):
    raise SystemExit(1)
PY2
  then break; fi
  sleep 0.1
  ((attempts += 1))
done
if (( attempts >= 300 )); then
  echo "new replica never entered committed ISR" >&2
  exit 1
fi
finalize_reassignment
wait_assignment 'broker-1,broker-2,broker-3'

output="$("$CLIENT_BIN" consume -addr "127.0.0.1:${CLIENT_PORTS[2]}" -topic orders -partition 0 -offset 0 -max 10)"
grep -Fq 'value="controller-committed-order"' <<<"$output"
grep -Fq 'value="reassignment-catchup-record"' <<<"$output"
echo "New replica caught up, entered committed ISR, and served both committed records."
echo "Three-process staged replica reassignment and catch-up smoke test passed."
