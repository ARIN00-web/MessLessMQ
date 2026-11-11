# MessLessMQ — Final Architecture Review

## 1. What is integrated into the broker executable

The `cmd/broker` entrypoint loads validated environment configuration, opens a durable topic catalog and partition logs, creates the shared dispatcher, and serves the versioned binary protocol. With peer configuration enabled, it also starts a separate inter-broker listener, seed membership discovery, static replication handlers, and (when fixed voters are configured) a durable controller election runtime.

The current runtime includes:

- TCP client listener with length-prefixed binary framing and request deadlines.
- Persistent topic/partition catalog and append-only partition logs.
- PRODUCE API v1 with deterministic key routing, message key persistence, and `none`, `leader`, and `all` acknowledgement modes.
- CONSUME API v1 and v2; v2 returns key/value, timestamp, partition, and offsets. Reads are limited by the local persisted exclusive high watermark.
- METADATA API v1 with known broker addresses and statically configured partition replica/leader assignments.
- Optional TLS/mTLS on the client listener, connection limits, IP-based rate limiting, metrics, health, and diagnostics.
- Graceful operation drain and partition-log close.
- Seed-based broker membership over a peer TCP listener, with durable cluster/broker identity pinning.
- Synchronous inter-process record replication and commit-marker propagation for static ordered replica assignments.
- Persistent per-partition high watermarks in `<DATA_DIR>/metadata/commits/`.
- A durable fixed-voter Raft controller participant in each configured broker process. Term, vote, log entries, and commit index are persisted under `<DATA_DIR>/metadata/controller/raft-state.json`.

The runtime integration test covers TCP produce, consume, metadata, and local restart recovery. The replication smoke test starts three OS processes with independent data directories, verifies follower record copies, checks committed-read filtering, and tests MinISR behavior. The controller election smoke test verifies one elected controller leader, replacement election after that process stops, and restart of the former leader as a follower using its persisted state.

## 2. Current distributed behavior

### Membership

Brokers join configured seeds and exchange in-memory membership snapshots. Broker identity is pinned to a cluster ID and broker ID on disk. There is no heartbeat/lease-based membership failure detector, removal protocol, or consensus-committed membership view. Peer RPCs are plaintext and unauthenticated.

### Data replication

The live runtime replicates record batches and commit markers over `internal/interbroker`. Replica assignments are static environment configuration; each ordered assignment's first broker is the fixed leader. `acks=all` requires MinISR durable data acknowledgements, persists the leader high watermark, then requires MinISR commit-marker acknowledgements. Consume hides offsets greater than or equal to the local persisted high watermark. The in-process `replication.Manager` is not instantiated across processes; the running server uses a separate transport adapter.

A failed data quorum can leave uncommitted physical log tails. They are hidden from consumers, but automatic truncation/reconciliation is not implemented. An error 15 commit-marker quorum failure is an uncertain producer outcome because the leader watermark has already been persisted.

### Controller election

The running broker can participate in a fixed-voter Raft controller election over the peer TCP transport. The voter list is explicit and identical across members. Elections use randomized timeouts and majority voting; leaders send AppendEntries heartbeats. Each controller persists term, vote, log, and commit index. On restart, role and leader identity are discarded and the voter rejoins as a follower. A three-process smoke test kills the elected controller leader, observes a replacement leader, restarts the old process from its original data directory, and verifies that it rejoins as a follower.

This is currently **controller election and heartbeat durability**, not a complete controller-driven cluster metadata service. The server does not yet expose a proposal path for broker registration or partition assignment commands, and the runtime does not apply committed controller metadata to partition assignments. The controller leader is distinct from a partition leader. Controller election does not yet promote a new data-partition leader when the statically configured partition leader dies.

### Client and consumer APIs

The client router remains a package-level metadata cache; the CLI connects to the broker address supplied by the user and does not automatically route to partition leaders. Consumer group membership, group coordination, and committed consumer-offset APIs are not exposed end-to-end through the public TCP protocol. The CLI consume operation is an explicit stateless partition/offset fetch.

## 3. Storage and message format

The topic/partition catalog is persisted under `metadata/topics.json`; partition logs are stored below `logs/`. Runtime-produced records wrap key/value data in the CRC32-checked `MLMQMSG1` envelope. CONSUME v2 unwraps the envelope; API v1 remains compatible with the original value-only response. Legacy raw records are returned as values with nil keys.

The catalog is persistent, but topic create/delete and partition-count changes are not exposed as a live administrative API. A persisted topic's partition count is immutable at startup; a conflicting bootstrap definition is rejected.

A produce batch is not transactional. A disk error partway through a multi-record batch can leave earlier records appended. In replicated mode, `acks=leader` syncs only the leader log and leaves the tail uncommitted; a later successful `acks=all` may replicate and commit that contiguous prefix. Error 14 means data quorum was not reached and the leader watermark was not advanced. Error 15 means the leader watermark was persisted but commit-marker quorum confirmation is uncertain.

## 4. Security and operations

- TLS/mTLS can protect the client broker listener when configured. The inter-broker peer listener is currently plaintext and unauthenticated.
- Request rate limiting is active and keyed by remote IP, not authenticated principal.
- API-key authentication and ACL authorization packages are not wired into the live custom TCP request path.
- The CLI supports plaintext TCP only.
- HTTP admin endpoints are unauthenticated; bind `MESSLESSMQ_ADMIN_ADDRESS` only to loopback or a trusted private management network.
- Metrics expose request/error/produce/consume and latency observations. Health checks inspect local partition-log access.
- Graceful shutdown stops accepting new operations, drains in-flight work until the configured deadline, and closes topic logs.

## 5. Deployment assets

- Docker image runs as a non-root user and writes below `/var/lib/messlessmq`.
- Compose and Kubernetes configure one cluster ID, unique broker IDs, peer listeners, advertised peer addresses, seed discovery, and a fixed three-voter controller set.
- Each broker has a distinct durable volume/PVC for logs, commit state, identity, and controller Raft state.
- Default deployment manifests **do not** set `MESSLESSMQ_REPLICA_ASSIGNMENTS`; they run membership and controller election, but message replication is enabled only after identical static replica assignments and MinISR are configured on all brokers.
- Kubernetes TCP probes only show that the process accepts a connection; they do not establish data replication health, commit convergence, or controller quorum readiness.

Persistent volumes protect each broker's local state where the storage provider honors the PVC. Static assignments enable synchronous record and commit-marker replication. The controller can elect a leader, but it cannot yet update assignments or promote partition leaders.

## 6. Strong engineering aspects

- Explicit TCP framing and versioned binary APIs.
- Append-only segmented storage, indexes, retention, and startup recovery.
- Persistent keyed records and CRC32 envelope validation.
- Explicit acknowledgements, static replica replication, MinISR, and committed-read boundaries.
- Durable controller term/vote/log/commit state and real multi-process fixed-voter elections.
- Separate modeling of controller quorum and data replication quorum.
- Race-tested packages and repeatable multi-process smoke scripts.

## 7. Remaining work before a distributed production release

1. Add authentication and TLS to inter-broker traffic, plus heartbeat/lease membership health and removal.
2. Add a live controller proposal API for broker registration and partition assignments; replicate and commit those commands through the Raft log.
3. Apply committed metadata on every broker and make it authoritative for replica assignments and leader epochs.
4. Implement Raft per-peer replication progress/backtracking, current-term commit rules for actual metadata proposals, quorum-loss tests, and safe voter reconfiguration.
5. Connect controller decisions to partition leader promotion, dynamic ISR removal, replica reassignment, catch-up, and safe old-leader fencing.
6. Route produce/fetch clients to leaders using refreshed committed cluster metadata.
7. Expose topic administration, group join/heartbeat/leave, offset commit/fetch, and group-aware consumption through versioned wire APIs.
8. Integrate API-key authentication and ACL authorization into the TCP request pipeline and provide a TLS-capable client.
9. Define idempotency/retry semantics across partial appends, uncertain commit outcomes, and server restarts.
10. Add multi-process data-plane failover tests: kill a partition leader, continue writes/reads through a promoted replica, restart the former leader, and verify catch-up.
11. Measure end-to-end throughput, p95/p99 latency, replication lag, recovery time, and durability under process, network, and disk faults.

## 8. Delivery-semantics caution

The codebase contains at-most-once, at-least-once, idempotent-producer, duplicate-detection, and local effect-ledger models. The runtime PRODUCE API does not expose a complete idempotent producer/session lifecycle. Do not describe it as a general exactly-once transaction system. Exactly-once external side effects require the destination to participate in a transaction or provide its own idempotency/deduplication semantics.

## 9. Final assessment

MessLessMQ has a persistent broker runtime, real inter-process synchronous record replication with durable commit markers under static assignments, and real fixed-voter controller elections with durable term/vote/log/commit state. It is not yet an autonomous distributed broker: controller metadata proposals do not drive partition assignments, and controller failover does not yet trigger data-partition leader promotion or client rerouting. It should not be presented as a production-ready Kafka replacement until those integration and security gaps are closed and tested end to end.
