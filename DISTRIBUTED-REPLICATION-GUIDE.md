# MessLessMQ — Distributed Replication v1 + Commit Boundary

This milestone connects broker-to-broker record replication to the actual peer TCP listener and adds a durable exclusive high watermark on every replica. Consumers now read only records below the locally persisted high watermark.

It is still a static-assignment replication system, not a complete Kafka-style controller/ISR implementation.

## What is connected

- Seed-based membership discovery provides peer addresses.
- A static replica assignment maps every topic partition to an ordered replica list. The first broker is the fixed leader at epoch 1.
- `acks=all` appends and syncs on the leader, queries follower log/commit state, verifies the latest common record, sends bounded batches, and waits for durable follower append acknowledgements.
- After the leader plus at least `MESSLESSMQ_MIN_ISR - 1` followers have persisted the required log prefix, the leader atomically persists its own commit watermark and sends a commit marker to the followers that acknowledged the data.
- A follower accepts a commit marker only from the configured partition leader and epoch, and only when its local log end is at least the requested high watermark.
- Commit markers are persisted using atomic file replacement, file `fsync`, rename, and directory `fsync` under `DATA_DIR/metadata/commits/`.
- CONSUME checks the persisted high watermark. It never returns offsets greater than or equal to that boundary and reports the committed end rather than the physical log end.
- A follower whose commit watermark is ahead of the leader's current watermark is rejected during replication status validation.
- Identical repeated record offsets remain idempotent. Divergent records and offset gaps remain errors.

## Configuration

All brokers in the cluster must use the same cluster ID, topic catalog, replica assignment string, and MinISR setting. Each broker must have its own broker ID, client listener, peer listener, advertised addresses, and data directory.

Example shared settings:

```bash
MESSLESSMQ_CLUSTER_ID=messlessmq-lab
MESSLESSMQ_BOOTSTRAP_TOPICS=orders:3
MESSLESSMQ_REPLICA_ASSIGNMENTS='orders:0=broker-1,broker-2,broker-3;orders:1=broker-2,broker-3,broker-1;orders:2=broker-3,broker-1,broker-2'
MESSLESSMQ_MIN_ISR=2
```

Example broker 1 settings (adjust addresses and IDs for each process):

```bash
MESSLESSMQ_BROKER_ID=broker-1
MESSLESSMQ_LISTEN_ADDRESS=127.0.0.1:29092
MESSLESSMQ_ADVERTISE_ADDRESS=127.0.0.1:29092
MESSLESSMQ_PEER_LISTEN_ADDRESS=127.0.0.1:29192
MESSLESSMQ_PEER_ADVERTISE_ADDRESS=127.0.0.1:29192
MESSLESSMQ_SEEDS=broker-1@127.0.0.1:29192
MESSLESSMQ_ADMIN_ADDRESS=127.0.0.1:29292
MESSLESSMQ_DATA_DIR=./data/broker-1
```

Broker 2 and broker 3 use their own ports and data directories and seed from broker 1. The seed list, advertised addresses, and port mappings must be reachable from every broker.

## Commit and acknowledgement semantics

The high watermark is **exclusive**: if `HighWatermark = 10`, offsets `0` through `9` are committed and offset `10` is not committed.

| Mode | Assigned multi-replica partition | Standalone broker |
|---|---|---|
| `acks=none` | Appends locally without fsync or follower replication. It does not advance the commit watermark. | The runtime syncs the local append and advances the local commit watermark. |
| `acks=leader` | Syncs the leader's local log but does not advance the commit watermark or wait for followers. The record is hidden from consumers until a later quorum append commits the prefix. | Syncs locally and commits the record because the local broker is the entire replica set. |
| `acks=all` | Syncs locally, catches followers up, requires `MinISR` durable data acknowledgements, persists the leader watermark, then requires `MinISR` durable commit-marker acknowledgements. | Syncs locally and advances the local commit watermark. |

A subsequent successful `acks=all` operation can replicate and commit earlier `acks=leader` records as part of the same contiguous prefix.

### Error codes

- **14 — insufficient ISR:** The data append did not reach `MinISR`; the leader does not advance its commit watermark. A local or follower uncommitted tail may remain, but consumers cannot read it.
- **15 — commit outcome uncertain:** The leader already persisted its new watermark, but the commit-marker acknowledgements did not reach `MinISR`. The append may be committed on the leader and possibly some followers. A producer must treat the outcome as unknown, not assume that retrying is duplicate-free.

The current runtime does not expose idempotent producer sessions end-to-end, so a retry after an uncertain result can create a duplicate.

## Committed reads

Every runtime CONSUME request checks the local commit store:

1. Read the partition's retained physical range.
2. Read the persisted exclusive high watermark.
3. Reject a requested offset greater than the high watermark.
4. Return only records whose offsets are smaller than the high watermark.
5. Report the high watermark as `log_end_offset` in the consume response.

The physical log may therefore contain a longer uncommitted tail than consumers can see.

## Run the three-process replication/commit test

```bash
chmod +x scripts/replication-smoke.sh
./scripts/replication-smoke.sh
```

The script:

1. Starts three independent broker processes with separate data directories and peer/client/admin listeners.
2. Waits until all membership views converge.
3. Produces with `acks=all` and verifies the record from both followers.
4. Produces with `acks=leader` and verifies the uncommitted record is hidden on the leader and a follower.
5. Produces a later `acks=all` record and verifies the pending prefix becomes visible only after commit markers are persisted.
6. Stops one follower and verifies `MinISR=2` still permits a committed write.
7. Stops both followers and verifies the next write is rejected with error 14 and remains hidden from leader consumers.

## Deliberate limitations

- In controller mode, replica assignments are sourced from the controller-committed metadata snapshot. If no explicit map is provided, the runtime assigns all fixed voters as replicas and rotates the initial leader by partition ID. Explicit ordered maps remain supported and must be consistent on every voter.
- The first replica in each initial assignment is the bootstrap leader at epoch 1. The live controller can now promote a safe replica from that same committed replica set after repeated failed status probes, committing a higher leader epoch. Dynamic replica-set changes are not implemented.
- The in-process `replication.Manager` is not instantiated across processes; the runtime uses a separate server-side synchronous transport adapter.
- Commit state is persisted locally on each broker, but there is no controller-coordinated reconciliation or quorum recovery after leader process loss.
- If the leader watermark is persisted but commit-marker quorum is not confirmed, error 15 reports an uncertain outcome. The watermark is not rolled back.
- A failed data quorum can leave an uncommitted tail. Reads are filtered, but automatic truncation/reconciliation of such tails is not implemented.
- Follower catch-up requires the follower to retain the leader's start offset. Snapshot/bootstrap for followers behind retention is not implemented.
- Catch-up checks the latest overlapping record, not a full-log digest.
- Inter-broker messages are plaintext and unauthenticated. Use only a trusted private network until peer authentication and TLS are integrated.
- The CLI automatically routes keyed PRODUCE requests using METADATA and refreshes/retries once after explicit stale-leader or controller-authority errors. Unkeyed produce and consumer fetch routing remain manual.
- A durable fixed-voter controller election runs across real broker processes; term, vote, log entries, and commit index are persisted per voter. The elected leader commits broker registrations and initial partition assignments, and each broker applies only the committed snapshot to METADATA and replication routing.
- Controller election and partition leader promotion are separate operations. The controller leader now promotes an assigned replica only when its commit metadata matches the old leader/epoch and its physical log end exactly equals the highest observed high watermark.

See `PARTITION-LEADER-FAILOVER-GUIDE.md` for the controller-driven failover algorithm, authority-lease fencing, keyed CLI routing, and the three-process failover/catch-up test. Remaining replication work includes dynamic ISR, staged reassignment, and a safe snapshot/rebootstrap path for divergent replicas.
