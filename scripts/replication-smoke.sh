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
CLIENT_BIN="$TEMP_ROOT/messlessmq-client"
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

echo "Building broker and client binaries..."
go build -o "$BROKER_BIN" ./cmd/broker
go build -o "$CLIENT_BIN" ./cmd/client

ASSIGNMENTS='orders:0=broker-1,broker-2,broker-3;orders:1=broker-2,broker-3,broker-1;orders:2=broker-3,broker-1,broker-2'

start_broker() {
  local index="$1"
  local id="$2"
  local client_port="${CLIENT_PORTS[$index]}"
  local peer_port="${PEER_PORTS[$index]}"
  local admin_port="${ADMIN_PORTS[$index]}"
  local seed_list="$3"
  local data_dir="$TEMP_ROOT/$id"

  env \
    MESSLESSMQ_BROKER_ID="$id" \
    MESSLESSMQ_CLUSTER_ID=messlessmq-replication-smoke \
    MESSLESSMQ_LISTEN_ADDRESS="127.0.0.1:$client_port" \
    MESSLESSMQ_ADVERTISE_ADDRESS="127.0.0.1:$client_port" \
    MESSLESSMQ_PEER_LISTEN_ADDRESS="127.0.0.1:$peer_port" \
    MESSLESSMQ_PEER_ADVERTISE_ADDRESS="127.0.0.1:$peer_port" \
    MESSLESSMQ_SEEDS="$seed_list" \
    MESSLESSMQ_PEER_JOIN_TIMEOUT=15s \
    MESSLESSMQ_ADMIN_ADDRESS="127.0.0.1:$admin_port" \
    MESSLESSMQ_DATA_DIR="$data_dir" \
    MESSLESSMQ_BOOTSTRAP_TOPICS=orders:3 \
    MESSLESSMQ_REPLICA_ASSIGNMENTS="$ASSIGNMENTS" \
    MESSLESSMQ_MIN_ISR=2 \
    MESSLESSMQ_LOG_LEVEL=warn \
    "$BROKER_BIN" >"$TEMP_ROOT/$id.log" 2>&1 &
  PIDS+=("$!")
}

wait_for_admin() {
  local port="$1"
  local log_file="$2"
  local attempts=0
  until curl -fsS "http://127.0.0.1:$port/health/live" >/dev/null 2>&1; do
    if (( attempts >= 150 )); then
      echo "broker admin endpoint on port $port did not become ready" >&2
      cat "$log_file" >&2
      exit 1
    fi
    sleep 0.1
    ((attempts += 1))
  done
}

wait_for_membership() {
  local attempts=0
  local snapshot
  while (( attempts < 150 )); do
    local all_ready=1
    for port in "${ADMIN_PORTS[@]}"; do
      snapshot="$(curl -fsS "http://127.0.0.1:$port/cluster/members" 2>/dev/null || true)"
      for id in broker-1 broker-2 broker-3; do
        if ! grep -Fq "\"id\":\"$id\"" <<<"$snapshot"; then
          all_ready=0
        fi
      done
    done
    if (( all_ready == 1 )); then
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

start_broker 0 broker-1 "broker-1@127.0.0.1:${PEER_PORTS[0]}"
wait_for_admin "${ADMIN_PORTS[0]}" "$TEMP_ROOT/broker-1.log"
start_broker 1 broker-2 "broker-1@127.0.0.1:${PEER_PORTS[0]}"
wait_for_admin "${ADMIN_PORTS[1]}" "$TEMP_ROOT/broker-2.log"
start_broker 2 broker-3 "broker-1@127.0.0.1:${PEER_PORTS[0]},broker-2@127.0.0.1:${PEER_PORTS[1]}"
wait_for_admin "${ADMIN_PORTS[2]}" "$TEMP_ROOT/broker-3.log"
wait_for_membership

metadata_output="$("$CLIENT_BIN" metadata -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders)"
grep -Fq 'partition=0 leader=broker-1 replicas=[broker-1 broker-2 broker-3]' <<<"$metadata_output"
grep -Fq 'partition=1 leader=broker-2 replicas=[broker-2 broker-3 broker-1]' <<<"$metadata_output"
grep -Fq 'partition=2 leader=broker-3 replicas=[broker-3 broker-1 broker-2]' <<<"$metadata_output"
echo "Metadata exposes the three configured partition leaders and replica lists."

# customer-7 hashes to orders partition 0 in the current producer partitioner.
"$CLIENT_BIN" produce -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -key customer-7 -value replicated-order-created -acks all

# Read the same offset from both followers. This is the key proof that the
# leader synchronously replicated to separate broker processes and data dirs.
for index in 1 2; do
  output="$("$CLIENT_BIN" consume -addr "127.0.0.1:${CLIENT_PORTS[$index]}" -topic orders -partition 0 -offset 0 -max 10)"
  grep -Fq 'key="customer-7"' <<<"$output"
  grep -Fq 'value="replicated-order-created"' <<<"$output"
  echo "broker-$((index + 1)) has the replicated record: $output"
done

# An acks=leader append is durable on the leader but remains uncommitted in a
# replicated partition until a later MinISR replication quorum commits it.
output="$($CLIENT_BIN produce -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -key customer-7 -value leader-only-pending -acks leader)"
grep -Fq 'offset=1' <<<"$output"
leader_pending="$($CLIENT_BIN consume -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -partition 0 -offset 1 -max 10)"
grep -Fq 'next_offset=1 log_end_offset=1' <<<"$leader_pending"
if grep -Fq 'leader-only-pending' <<<"$leader_pending"; then
  echo "leader exposed an uncommitted record" >&2
  exit 1
fi
follower_pending="$($CLIENT_BIN consume -addr "127.0.0.1:${CLIENT_PORTS[1]}" -topic orders -partition 0 -offset 1 -max 10)"
grep -Fq 'next_offset=1 log_end_offset=1' <<<"$follower_pending"
echo "Uncommitted leader tail is hidden from leader and follower consumers."

# The next acks=all operation catches the followers up through the preceding
# leader-only append and commits the full prefix with a durable commit marker.
output="$($CLIENT_BIN produce -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -key customer-7 -value commit-barrier -acks all)"
grep -Fq 'offset=2' <<<"$output"
follower_committed="$($CLIENT_BIN consume -addr "127.0.0.1:${CLIENT_PORTS[1]}" -topic orders -partition 0 -offset 1 -max 10)"
grep -Fq 'value="leader-only-pending"' <<<"$follower_committed"
grep -Fq 'value="commit-barrier"' <<<"$follower_committed"
echo "Followers expose records only after the durable commit marker arrives."

# With MinISR=2, one follower may be unavailable as long as the leader and
# one follower durably acknowledge the record.
kill -TERM "${PIDS[2]}"
wait "${PIDS[2]}" || true
output="$($CLIENT_BIN produce -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -key customer-7 -value min-isr-two-survived -acks all)"
grep -Fq 'offset=3' <<<"$output"
follower_output="$($CLIENT_BIN consume -addr "127.0.0.1:${CLIENT_PORTS[1]}" -topic orders -partition 0 -offset 3 -max 10)"
grep -Fq 'value="min-isr-two-survived"' <<<"$follower_output"
echo "MinISR=2 accepted the append with one follower unavailable."

# With both followers unavailable, the leader is only one acknowledgement and
# must reject the acks=all request when MinISR remains two.
kill -TERM "${PIDS[1]}"
wait "${PIDS[1]}" || true
set +e
rejected_output="$($CLIENT_BIN produce -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -key customer-7 -value should-not-be-acked -acks all 2>&1)"
rejected_status=$?
set -e
if (( rejected_status == 0 )) || ! grep -Fq 'error code 14' <<<"$rejected_output"; then
  echo "expected insufficient-ISR rejection, got status=$rejected_status output=$rejected_output" >&2
  exit 1
fi
leader_tail="$($CLIENT_BIN consume -addr "127.0.0.1:${CLIENT_PORTS[0]}" -topic orders -partition 0 -offset 4 -max 10)"
grep -Fq 'next_offset=4 log_end_offset=4' <<<"$leader_tail"
if grep -Fq 'should-not-be-acked' <<<"$leader_tail"; then
  echo "leader exposed a record that did not reach MinISR" >&2
  exit 1
fi
echo "MinISR=2 correctly rejected the append when both followers were unavailable, and the uncommitted record remained hidden."

echo "Three-process synchronous replication and MinISR smoke test passed."
