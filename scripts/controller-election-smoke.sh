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

CLIENT_PORTS=(29392 29393 29394)
PEER_PORTS=(29492 29493 29494)
ADMIN_PORTS=(29592 29593 29594)
ALL_PORTS=("${CLIENT_PORTS[@]}" "${PEER_PORTS[@]}" "${ADMIN_PORTS[@]}")
for port in "${ALL_PORTS[@]}"; do
  if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
    echo "required test port is already in use: $port" >&2
    exit 1
  fi
done

TEMP_ROOT="$(mktemp -d)"
BROKER_BIN="$TEMP_ROOT/messlessmq-broker"
VOTERS="broker-1,broker-2,broker-3"
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

echo "Building broker binary..."
go build -o "$BROKER_BIN" ./cmd/broker

start_broker() {
  local index="$1"
  local id="$2"
  local seeds="$3"
  local data_dir="$TEMP_ROOT/$id-data"
  local log_file="$TEMP_ROOT/$id.log"

  env \
    MESSLESSMQ_BROKER_ID="$id" \
    MESSLESSMQ_CLUSTER_ID=messlessmq-controller-election-smoke \
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
    MESSLESSMQ_ADMIN_ADDRESS="127.0.0.1:${ADMIN_PORTS[$index]}" \
    MESSLESSMQ_DATA_DIR="$data_dir" \
    MESSLESSMQ_BOOTSTRAP_TOPICS=orders:1 \
    MESSLESSMQ_LOG_LEVEL=warn \
    "$BROKER_BIN" >"$log_file" 2>&1 &
  local pid=$!
  PIDS[$index]="$pid"
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

wait_for_membership() {
  local attempts=0 snapshot port id ready
  while (( attempts < 150 )); do
    ready=1
    for port in "${ADMIN_PORTS[@]}"; do
      snapshot="$(curl -fsS "http://127.0.0.1:$port/cluster/members" 2>/dev/null || true)"
      for id in broker-1 broker-2 broker-3; do
        if ! grep -Fq "\"id\":\"$id\"" <<<"$snapshot"; then ready=0; fi
      done
    done
    if (( ready == 1 )); then
      echo "All broker membership views converged."
      return 0
    fi
    sleep 0.1
    ((attempts += 1))
  done
  echo "membership did not converge" >&2
  for id in broker-1 broker-2 broker-3; do cat "$TEMP_ROOT/$id.log" >&2; done
  exit 1
}

wait_for_leader() {
  local exclude_index="${1:--1}" attempts=0
  local status_json leader_id
  while (( attempts < 150 )); do
    local status_args=()
    local index
    for index in 0 1 2; do
      if (( index == exclude_index )) || [[ -z "${PIDS[$index]:-}" ]]; then continue; fi
      status_args+=("$(curl -fsS "http://127.0.0.1:${ADMIN_PORTS[$index]}/controller/status" 2>/dev/null || true)")
    done
    if (( ${#status_args[@]} > 0 )); then
      leader_id="$(python3 - "${status_args[@]}" <<'PY'
import json, sys
items=[]
try:
    items=[json.loads(value) for value in sys.argv[1:] if value]
except Exception:
    print("")
    raise SystemExit(0)
if not items or not all(item.get("ready") for item in items):
    print("")
    raise SystemExit(0)
leaders=[item for item in items if item.get("role")=="leader"]
leader_ids={item.get("leader_id") for item in items if item.get("leader_id")}
if len(leaders)==1 and len(leader_ids)==1 and leaders[0].get("broker_id") in leader_ids:
    print(leaders[0]["broker_id"])
else:
    print("")
PY
)"
      if [[ -n "$leader_id" ]]; then
        echo "$leader_id"
        return 0
      fi
    fi
    sleep 0.1
    ((attempts += 1))
  done
  echo "controller quorum did not converge on exactly one leader" >&2
  local index
  for index in 0 1 2; do
    if [[ -f "$TEMP_ROOT/broker-$((index + 1)).log" ]]; then
      echo "--- broker-$((index + 1)) log ---" >&2
      cat "$TEMP_ROOT/broker-$((index + 1)).log" >&2
    fi
  done
  exit 1
}

assert_rejoined_view() {
  local port="$1" expected_leader="$2" attempts=0 status_json
  while (( attempts < 100 )); do
    status_json="$(curl -fsS "http://127.0.0.1:$port/controller/status" 2>/dev/null || true)"
    if python3 - "$status_json" "$expected_leader" <<'PY'
import json, sys
try:
    status=json.loads(sys.argv[1])
except Exception:
    raise SystemExit(1)
raise SystemExit(0 if status.get("ready") and status.get("leader_id")==sys.argv[2] else 1)
PY
    then return 0; fi
    sleep 0.1
    ((attempts += 1))
  done
  echo "controller view on port $port did not converge to leader $expected_leader" >&2
  return 1
}

start_broker 0 broker-1 ""
wait_for_admin "${ADMIN_PORTS[0]}" "$TEMP_ROOT/broker-1.log"
start_broker 1 broker-2 "broker-1@127.0.0.1:${PEER_PORTS[0]}"
wait_for_admin "${ADMIN_PORTS[1]}" "$TEMP_ROOT/broker-2.log"
start_broker 2 broker-3 "broker-1@127.0.0.1:${PEER_PORTS[0]},broker-2@127.0.0.1:${PEER_PORTS[1]}"
wait_for_admin "${ADMIN_PORTS[2]}" "$TEMP_ROOT/broker-3.log"
wait_for_membership

leader_id="$(wait_for_leader -1)"
echo "Initial controller leader: $leader_id"

case "$leader_id" in
  broker-1) leader_index=0 ;;
  broker-2) leader_index=1 ;;
  broker-3) leader_index=2 ;;
  *) echo "unexpected leader ID: $leader_id" >&2; exit 1 ;;
esac
leader_pid="${PIDS[$leader_index]}"
kill -TERM "$leader_pid"
wait "$leader_pid" || true
PIDS[$leader_index]=""
echo "Stopped controller leader $leader_id."

new_leader="$(wait_for_leader "$leader_index")"
if [[ "$new_leader" == "$leader_id" ]]; then
  echo "failed controller leader was incorrectly retained as leader" >&2
  exit 1
fi
echo "New controller leader elected after failure: $new_leader"

# Restart the old process with the same persistent directory and same fixed voter set.
old_seed=""
if [[ "$leader_id" != "broker-1" ]]; then old_seed="broker-1@127.0.0.1:${PEER_PORTS[0]}"; fi
if [[ "$leader_id" == "broker-1" ]]; then
  old_seed="broker-2@127.0.0.1:${PEER_PORTS[1]},broker-3@127.0.0.1:${PEER_PORTS[2]}"
fi
start_broker "$leader_index" "$leader_id" "$old_seed"
wait_for_admin "${ADMIN_PORTS[$leader_index]}" "$TEMP_ROOT/$leader_id.log"

for index in 0 1 2; do
  assert_rejoined_view "${ADMIN_PORTS[$index]}" "$new_leader"
done

rejoined_leader="$(wait_for_leader -1)"
if [[ "$rejoined_leader" != "$new_leader" ]]; then
  echo "leader changed unexpectedly after the old controller rejoined: $rejoined_leader" >&2
  exit 1
fi
old_status="$(curl -fsS "http://127.0.0.1:${ADMIN_PORTS[$leader_index]}/controller/status")"
python3 - "$old_status" "$new_leader" <<'PY'
import json, sys
status=json.loads(sys.argv[1])
if status.get("role") != "follower" or status.get("leader_id") != sys.argv[2]:
    raise SystemExit("restarted controller did not rejoin as follower: %r" % status)
PY

echo "Old controller restarted from durable state and rejoined as a follower."
echo "Three-process durable controller election and failover smoke test passed."
