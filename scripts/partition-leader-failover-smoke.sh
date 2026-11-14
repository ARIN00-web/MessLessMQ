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

CLIENT_PORTS=(29992 29993 29994)
PEER_PORTS=(30092 30093 30094)
ADMIN_PORTS=(30192 30193 30194)
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
PIDS=("" "" "")

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
    MESSLESSMQ_CLUSTER_ID=messlessmq-partition-failover-smoke \
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

wait_for_membership_and_metadata() {
  local attempts=0 port snapshot
  while (( attempts < 200 )); do
    local ready=1
    for port in "${ADMIN_PORTS[@]}"; do
      snapshot="$(curl -fsS "http://127.0.0.1:$port/controller/metadata" 2>/dev/null || true)"
      if ! python3 - "$snapshot" <<'PY'
import json, sys
try:
    value=json.loads(sys.argv[1])
except Exception:
    raise SystemExit(1)
ids={b.get("id") for b in value.get("brokers") or []}
parts=value.get("partitions") or []
if ids != {"broker-1","broker-2","broker-3"} or len(parts) != 3:
    raise SystemExit(1)
PY
      then ready=0; break; fi
    done
    if (( ready == 1 )); then
      echo "All brokers applied committed membership and partition metadata."
      return 0
    fi
    sleep 0.1
    ((attempts += 1))
  done
  echo "initial metadata did not converge" >&2
  for id in broker-1 broker-2 broker-3; do cat "$TEMP_ROOT/$id.log" >&2 || true; done
  exit 1
}

wait_for_partition_leader() {
  local expected_exclude="$1" attempts=0 port body leader
  while (( attempts < 250 )); do
    for port in "${ADMIN_PORTS[1]}" "${ADMIN_PORTS[2]}"; do
      body="$(curl -fsS "http://127.0.0.1:$port/controller/metadata" 2>/dev/null || true)"
      leader="$(python3 - "$body" "$expected_exclude" <<'PY'
import json, sys
try:
    data=json.loads(sys.argv[1])
except Exception:
    print(""); raise SystemExit(0)
for part in data.get("partitions") or []:
    key=part.get("key") or {}
    if key.get("topic")=="orders" and key.get("partition")==0:
        candidate=part.get("leader", "")
        if candidate and candidate != sys.argv[2]: print(candidate)
        else: print("")
        break
else: print("")
PY
)"
      if [[ -n "$leader" ]]; then
        echo "$leader"
        return 0
      fi
    done
    sleep 0.1
    ((attempts += 1))
  done
  echo "partition leader did not move away from $expected_exclude" >&2
  for id in broker-2 broker-3; do cat "$TEMP_ROOT/$id.log" >&2; done
  return 1
}


start_broker 0 broker-1 ""
wait_for_admin "${ADMIN_PORTS[0]}" "$TEMP_ROOT/broker-1.log"
start_broker 1 broker-2 "broker-1@127.0.0.1:${PEER_PORTS[0]}"
wait_for_admin "${ADMIN_PORTS[1]}" "$TEMP_ROOT/broker-2.log"
start_broker 2 broker-3 "broker-1@127.0.0.1:${PEER_PORTS[0]},broker-2@127.0.0.1:${PEER_PORTS[1]}"
wait_for_admin "${ADMIN_PORTS[2]}" "$TEMP_ROOT/broker-3.log"
wait_for_membership_and_metadata

echo "Initial partition leader: broker-1"
"$CLIENT_BIN" produce -addr "127.0.0.1:${CLIENT_PORTS[2]}" -topic orders -key customer-7 -value before-partition-failover -acks all
for index in 1 2; do
  output="$("$CLIENT_BIN" consume -addr "127.0.0.1:${CLIENT_PORTS[$index]}" -topic orders -partition 0 -offset 0 -max 10)"
  grep -Fq 'value="before-partition-failover"' <<<"$output"
done
echo "Committed record is present on both followers before failure."

kill -TERM "${PIDS[0]}"
wait "${PIDS[0]}" || true
PIDS[0]=""
echo "Stopped partition leader broker-1."

new_leader="$(wait_for_partition_leader broker-1)"
echo "New committed partition leader: $new_leader"
"$CLIENT_BIN" produce -addr "127.0.0.1:${CLIENT_PORTS[1]}" -topic orders -key customer-7 -value after-partition-failover -acks all
follower_index=1
if [[ "$new_leader" == "broker-2" ]]; then follower_index=2; fi
follower_output="$("$CLIENT_BIN" consume -addr "127.0.0.1:${CLIENT_PORTS[$follower_index]}" -topic orders -partition 0 -offset 1 -max 10)"
grep -Fq 'value="after-partition-failover"' <<<"$follower_output"
echo "Surviving cluster produced and consumed after partition leader promotion."

# Restart the old leader with its original data and controller state.
start_broker 0 broker-1 "broker-2@127.0.0.1:${PEER_PORTS[1]},broker-3@127.0.0.1:${PEER_PORTS[2]}"
wait_for_admin "${ADMIN_PORTS[0]}" "$TEMP_ROOT/broker-1.log"

attempts=0
while (( attempts < 150 )); do
  old_metadata="$(curl -fsS "http://127.0.0.1:${ADMIN_PORTS[0]}/controller/metadata" 2>/dev/null || true)"
  if python3 - "$old_metadata" "$new_leader" <<'PY'
import json, sys
try: data=json.loads(sys.argv[1])
except Exception: raise SystemExit(1)
for part in data.get("partitions") or []:
    key=part.get("key") or {}
    if key.get("topic")=="orders" and key.get("partition")==0:
        raise SystemExit(0 if part.get("leader")==sys.argv[2] and part.get("leader_epoch",0)>=2 else 1)
raise SystemExit(1)
PY
  then break; fi
  sleep 0.1
  ((attempts += 1))
done
if (( attempts >= 150 )); then
  echo "old leader did not apply promoted controller metadata" >&2
  cat "$TEMP_ROOT/broker-1.log" >&2
  exit 1
fi
echo "Old broker rejoined with the new committed leader epoch."

# The next append causes the recovered old leader to catch up before the new
# record is committed on MinISR=2.
"$CLIENT_BIN" produce -addr "127.0.0.1:${CLIENT_PORTS[2]}" -topic orders -key customer-7 -value after-old-leader-rejoin -acks all
old_output="$("$CLIENT_BIN" consume -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -partition 0 -offset 1 -max 10)"
grep -Fq 'value="after-partition-failover"' <<<"$old_output"
grep -Fq 'value="after-old-leader-rejoin"' <<<"$old_output"
echo "Recovered old leader caught up from the new leader's log."
echo "Three-process partition leader failover, client metadata, and replica recovery smoke test passed."
