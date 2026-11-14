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

The live runtime replicates record batches and commit markers over `internal/interbroker`. In controller mode, replica assignments come from the committed controller snapshot; initial assignments are derived from sorted fixed voter IDs or supplied as one identical explicit map on all brokers. Each ordered assignment's first broker is the fixed initial leader. `acks=all` requires MinISR durable data acknowledgements, persists the leader high watermark, then requires MinISR commit-marker acknowledgements. Consume hides offsets greater than or equal to the local persisted high watermark. The in-process `replication.Manager` is not instantiated across processes; the running server uses a separate transport adapter.

A failed data quorum can leave uncommitted physical log tails. They are hidden from consumers, but automatic truncation/reconciliation is not implemented. An error 15 commit-marker quorum failure is an uncertain producer outcome because the leader watermark has already been persisted.

### Controller election

The running broker can participate in a fixed-voter Raft controller election over the peer TCP transport. The voter list is explicit and identical across members. Elections use randomized timeouts and majority voting; leaders send AppendEntries heartbeats. Each controller persists term, vote, log, and commit index. On restart, role and leader identity are discarded and the voter rejoins as a follower. A three-process smoke test kills the elected controller leader, observes a replacement leader, restarts the old process from its original data directory, and verifies that it rejoins as a follower.

The runtime now has a fixed-voter controller proposal path for broker registration and initial partition assignment. Proposals are replicated to a majority, committed under the current-term rule, and applied from the committed snapshot on each broker. `GET /controller/metadata` exposes the committed snapshot; the ordinary METADATA API and replication adapter use the applied assignment view. The controller leader is distinct from a partition leader.

The current runtime also performs controller-driven partition leader promotion. The elected controller probes the assigned partition leader three times (350 ms per probe) before attempting failover. It queries the other assigned replicas and only considers a replica whose commit-store leader ID and epoch match the old assignment, whose high watermark is the highest observed among eligible replicas, and whose physical log end is exactly that high watermark. A replica with an uncommitted physical suffix is not eligible. The controller commits the new leader with an incremented leader epoch; brokers apply the committed assignment and update their local commit state. A failed or partitioned old leader is fenced from data writes when its controller-authority lease expires. The controller lease is renewed by a majority heartbeat for a leader or accepted AppendEntries from the current leader for a follower.

The keyed CLI producer now fetches METADATA, mirrors the broker's FNV-1a key partitioner, connects to the advertised partition leader, and refreshes metadata once after a not-leader or controller-authority error. Unkeyed CLI produce is not automatically routed because the broker's round-robin choice cannot be inferred from a message key. The CLI consumer remains an explicit broker/partition/offset fetch. The three-process partition failover smoke test kills a partition leader, verifies a committed replacement leader and epoch, produces through refreshed client metadata, restarts the former leader, and verifies it catches up from the new leader's log.

### Client and consumer APIs

The CLI keyed producer fetches METADATA, mirrors the broker's FNV-1a partition selection, routes to the committed partition leader's advertised address, and refreshes metadata/retries once after a stale-leader or authority error. Unkeyed produce is not auto-routed because partition selection is broker-local round robin. The CLI consume operation remains an explicit stateless broker/partition/offset fetch. Consumer group membership, group coordination, and committed consumer-offset APIs are not exposed end-to-end through the public TCP protocol.

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
- Default deployment manifests configure a fixed three-voter controller and `MESSLESSMQ_MIN_ISR=2`. When no explicit replica map is supplied, the elected controller commits a deterministic assignment containing all fixed voters for every bootstrap partition, with the initial leader rotated by partition ID.
- Kubernetes TCP probes only show that the process accepts a connection; they do not establish data replication health, commit convergence, or controller quorum readiness.

Persistent volumes protect each broker's local state where the storage provider honors the PVC. Controller-committed replica assignments enable synchronous record and commit-marker replication. The controller commits broker registrations and partition assignments, and can promote a safe in-set partition replica after leader failure. These manifests still represent a fixed voter set and do not provide staged replica reassignment or dynamic ISR membership.

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
2. Add dynamic ISR membership, staged replica reassignment, and a safe snapshot/rebootstrap path for replicas with divergent uncommitted suffixes.
3. Improve Raft with per-peer replication progress/backtracking, follower snapshot/bootstrap, stronger quorum-loss recovery, and safe voter reconfiguration.
4. Extend client routing to unkeyed produce, consumer fetch, and a reusable client metadata cache; define bounded retry and uncertain-outcome behavior.
5. Expose topic administration, group join/heartbeat/leave, offset commit/fetch, and group-aware consumption through versioned wire APIs.
6. Integrate API-key authentication and ACL authorization into the TCP request pipeline, and authenticate/encrypt peer RPCs with TLS/mTLS.
7. Define idempotency/retry semantics across partial appends, uncertain commit outcomes, and server restarts.
8. Measure end-to-end throughput, p95/p99 latency, replication lag, recovery time, and durability under process, network, and disk faults.

## 8. Delivery-semantics caution

The codebase contains at-most-once, at-least-once, idempotent-producer, duplicate-detection, and local effect-ledger models. The runtime PRODUCE API does not expose a complete idempotent producer/session lifecycle. Do not describe it as a general exactly-once transaction system. Exactly-once external side effects require the destination to participate in a transaction or provide its own idempotency/deduplication semantics.

## 9. Final assessment

MessLessMQ has a persistent broker runtime, real inter-process synchronous record replication with durable commit markers, fixed-voter controller elections with durable term/vote/log/commit state, controller-committed broker registrations and partition assignments, automatic in-set partition leader promotion, authority-lease fencing, and keyed CLI metadata refresh/routing. Three-process smoke tests exercise membership, replicated committed reads, controller leader failover, committed metadata, partition leader failover, and former-leader catch-up. It is a substantial distributed-systems implementation, but it is not yet a production-ready Kafka replacement: peer/admin traffic still needs authentication, dynamic ISR and staged reassignment are absent, divergent replicas require manual rebootstrap, client APIs are incomplete, and deeper Raft/network/disk chaos testing remains necessary.
