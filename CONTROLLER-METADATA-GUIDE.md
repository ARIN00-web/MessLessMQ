# Controller-Committed Cluster Metadata

This milestone connects the live broker processes to the durable fixed-voter controller log.

## What is committed

The elected controller leader proposes:

- broker registrations (`broker ID` and advertised client address);
- initial topic-partition assignments (`replica IDs`, initial partition leader, and leader epoch).

The leader replicates proposals to the configured fixed voter set, requires a majority, commits current-term entries, and applies the resulting metadata snapshot. Followers apply only the prefix covered by their persisted commit index. Brokers expose the committed snapshot at `GET /controller/metadata` and use the applied assignment registry for normal METADATA responses and inter-broker replication routing.

## Assignment bootstrap

Every voter must share the same `MESSLESSMQ_CLUSTER_ID`, `MESSLESSMQ_CONTROLLER_VOTERS`, bootstrap topic catalog, and `MESSLESSMQ_MIN_ISR`. Each broker needs its own durable data directory/PVC.

If `MESSLESSMQ_REPLICA_ASSIGNMENTS` is configured, all brokers must use the same ordered assignment map. The first broker in each list is the initial partition leader. Every replica must be a member of the configured fixed voter set.

If no explicit replica map is provided, the runtime sorts voter IDs and assigns all fixed voters as replicas for every bootstrap topic partition. The initial leader rotates by partition ID. For example, with voters `broker-1,broker-2,broker-3`:

- partition 0: leader broker-1, replicas broker-1/broker-2/broker-3;
- partition 1: leader broker-2, replicas broker-2/broker-3/broker-1;
- partition 2: leader broker-3, replicas broker-3/broker-1/broker-2.

This deterministic starter policy is not rack-aware, load-aware, or capacity-aware.

## Inspect committed metadata

With the admin listener bound to a trusted private interface:

```bash
curl http://127.0.0.1:9093/controller/status
curl http://127.0.0.1:9093/controller/metadata
```

The ordinary client METADATA API should return the same committed broker and partition assignment view after each broker has applied the committed snapshot.

## Test with three processes

```bash
./scripts/controller-metadata-smoke.sh
```

The script starts three independent broker executables and data directories, waits for committed metadata to converge, checks all three client METADATA APIs, produces a message with `acks=all` through the committed partition leader, then fetches that record from each follower's local log.

## Current safety and implementation boundaries

- The voter set is fixed for the lifetime of the persisted controller state. Voter reconfiguration is not implemented.
- Startup reconciliation creates missing initial assignments, but it never silently overwrites an existing committed assignment.
- Controller leadership and partition leadership are separate. This milestone commits the initial partition leader and replica set; automatic promotion after partition-leader failure is not implemented yet.
- There is no staged reassignment/catch-up workflow attached to controller metadata commands yet.
- The Raft transport currently uses full-log AppendEntries for the small fixed cluster. Per-peer next/match indexes, backtracking, snapshots, and large-log compaction remain future work.
- Peer RPC and the HTTP admin endpoints are plaintext/unauthenticated; keep them on a trusted private network.
- Each broker must retain its own `DATA_DIR`. Do not share one broker's local directory between processes.
- Set `MESSLESSMQ_MIN_ISR=2` for the three-broker examples if `acks=all` must require at least two in-sync replicas.

The controller metadata smoke test proves committed initial assignment and data replication across processes. It does not prove automatic partition-leader failover or production-grade Raft membership changes.
