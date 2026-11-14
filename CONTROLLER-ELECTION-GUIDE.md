# MessLessMQ — Durable Controller Election v1

This milestone adds a real fixed-voter controller election to the broker runtime. Each configured broker process owns one durable Raft participant. It is intentionally distinct from message partition leadership.

## Configure the same voter set on every broker

Every voter must use the same ordered/identical comma-separated voter ID set and a unique broker ID. The local broker ID must appear in the voter list.

```bash
MESSLESSMQ_CLUSTER_ID=messlessmq-lab
MESSLESSMQ_CONTROLLER_VOTERS=broker-1,broker-2,broker-3
MESSLESSMQ_ELECTION_TIMEOUT_MIN=900ms
MESSLESSMQ_ELECTION_TIMEOUT_MAX=1500ms
MESSLESSMQ_HEARTBEAT_INTERVAL=200ms
```

Peer membership must also be enabled using `MESSLESSMQ_PEER_LISTEN_ADDRESS`, `MESSLESSMQ_PEER_ADVERTISE_ADDRESS`, and, for joining brokers, `MESSLESSMQ_SEEDS`. The controller runtime waits until all fixed voter IDs are visible in its local membership snapshot before it begins elections.

## Persistent state

Each broker stores its controller state in its own data directory:

```text
<DATA_DIR>/metadata/controller/raft-state.json
```

The persisted state includes:

- Current Raft term.
- Voted-for broker ID in the current term.
- Controller log entries.
- Commit index.
- Broker identity and the fixed peer/voter set, used to reject accidental identity or voter-set changes.

Writes use a temporary file, file sync, atomic rename, and directory sync. Leader/follower role and known leader are deliberately not restored from disk; a restarted voter starts as a follower and learns the current leader through AppendEntries.

## Inspect election state

With `MESSLESSMQ_ADMIN_ADDRESS` configured, inspect:

```bash
curl -fsS http://127.0.0.1:9093/controller/status
```

The status reports broker ID, role, term, voted-for ID, known leader ID, last log index, commit index, applied index, configured voters, and how many configured voters are present in the local membership view.

`ready` means all configured voters are discovered locally. It does not mean a metadata proposal was committed, and it is not a replacement for the broker's health/readiness endpoint.

## Run the three-process election/failure test

```bash
chmod +x scripts/controller-election-smoke.sh
./scripts/controller-election-smoke.sh
```

The script starts three independent broker processes with separate data directories, waits for membership convergence and one controller leader, stops that leader process, waits for the remaining majority to elect a replacement, then restarts the former leader using its original durable state. It verifies that the restarted process rejoins as a follower and that all live controller status endpoints agree on the new leader.

The script requires `go`, `curl`, `nc`, and `python3`. It uses client ports `29392`–`29394`, peer ports `29492`–`29494`, and admin ports `29592`–`29594`; these must be free.

## Current boundaries

- The voter set is static. Changing it while reusing a controller data directory is rejected. Safe dynamic voter reconfiguration is not implemented.
- Peer RPCs are plaintext and unauthenticated. Use a private trusted network only.
- The runtime now proposes broker registrations and initial partition assignments through the durable controller log. A majority must store a proposal before the leader commits it; followers apply the committed prefix. See `CONTROLLER-METADATA-GUIDE.md`.
- Controller-committed assignments drive the initial METADATA response and data replication routing. If no explicit `MESSLESSMQ_REPLICA_ASSIGNMENTS` map is configured, every fixed voter is assigned as a replica and the initial leader rotates by partition ID over sorted voter IDs.
- The controller leader is not necessarily the leader of any message partition. They are different roles.
- The election smoke test kills a controller process. Data-partition leader failover is a separate controller-driven workflow covered by `scripts/partition-leader-failover-smoke.sh` and `PARTITION-LEADER-FAILOVER-GUIDE.md`.
- The fixed voter set is not dynamically reconfigured. The current peer RPCs are plaintext/unauthenticated, and the Raft transport has not yet implemented per-peer progress/backtracking or snapshot transfer.

The next hardening work is dynamic ISR/reassignment, replica snapshot/rebootstrap, authenticated peer RPCs, and deeper network/disk chaos testing. Partition leader promotion and keyed CLI metadata routing are now integrated; see `PARTITION-LEADER-FAILOVER-GUIDE.md`.
