# Distributed Systems Guarantees & Verification Reference

## Architecture & System Overview

MessLessMQ is a distributed, partitioned, replicated message broker implemented in Go. It provides durable storage, Raft-based controller metadata management, multi-broker cluster membership discovery, in-sync replica (ISR) tracking, partition leader failover, and client routing for external and containerized workloads.

---

## Implemented Boundaries & Hardened Invariants

### 1. Storage & Persistence Boundaries
- **Advisory Directory Locking:** An exclusive advisory lock (`flock` on Unix/macOS/Linux) on `<DATA_DIR>/.lock` prevents multiple broker processes from concurrently opening, modifying, or corrupting the same data directory.
- **Durable Disk Persistence:** `AppendAtSync` synchronously writes and issues `fsync` (`file.Sync()`) to the active segment on disk.
- **Tail Recovery:** During startup, `recoverSegmentTail` detects and safely truncates partial or incomplete trailing record headers and payloads at EOF caused by sudden crashes, and rebuilds `.index` and `.timeindex` sidecars from verified segment records.
- **Restart Monotonicity:** Offsets remain strictly monotonic across segment rolls and process restarts.

### 2. Controller & Raft Quorum Invariants
- **Quorum Requirement:** Controller elections and metadata command commits strictly require a majority vote (`(N/2)+1`) of the configured voter set.
- **Log Matching & Conflict Resolution:** Follower logs are strictly aligned to the leader's log; uncommitted suffixes conflicting with the leader's term or index are truncated, while committed suffixes are immutable.
- **Durable Raft State:** `DurableNode` persists `currentTerm`, `votedFor`, committed `log`, and `commitIndex` atomically with sync and directory sync before replying to RPCs. Role and leader ID remain volatile and are re-established upon restart.
- **Authoritative Controller Metadata:** Committed Raft metadata is authoritative across all brokers. Broker client and peer addresses are proposed via `CommandUpdateBrokerAddress` and committed across the cluster. Static bootstrap configurations serve only as initial seeds for unassigned topics.
- **Authority Lease Fencing:** A partition leader must verify active controller authority lease (renewed via majority heartbeats within $2 \times \text{heartbeat interval}$) to accept distributed appends. If lease expires, the broker fails closed with `protocol.ErrControllerAuthorityUnavailable`.

### 3. Replication & Acknowledgement Semantics
- **`acks=none` (Fire & Forget):** The record is written to the leader's active segment buffer; returns immediately without waiting for disk sync or replica propagation.
- **`acks=leader` (Local Durable):** Appends to the leader's active segment and issues `file.Sync()`. Guarantees local disk persistence before acknowledging the producer. Does not wait for replica propagation.
- **`acks=all` (Quorum Durable):**
  1. Serializes appends per partition under an exclusive partition lock.
  2. Appends and syncs locally on the partition leader (`AppendAtSync`).
  3. Replicates bounded batches to all assigned replicas over the inter-broker TCP transport.
  4. Waits for durable follower append acknowledgements and verifies that $\text{acknowledged} \ge \text{MinISR}$. If not, returns `protocol.ErrInsufficientISR` without advancing the commit watermark.
  5. Advances the leader's high watermark and writes a durable commit state to disk (`commits.Advance`).
  6. Dispatches commit markers to followers; requires at least $\text{MinISR}$ commit marker confirmations. If commit marker quorum fails, returns `protocol.ErrCommitUncertain`.
- **Committed-Read Isolation:** Consumers are strictly restricted to reading records strictly below the exclusive high watermark (`offset < HighWatermark`). Uncommitted physical records are hidden from consumers on both leaders and followers.

### 4. Partition Failover & ISR Lifecycle
- **Health Probing:** The elected controller monitors partition leaders and replicas. A partition leader is marked failed only after 3 consecutive missed probes (350 ms timeout).
- **Safe Candidate Selection:** The controller selects replacement leaders from assigned replicas that prove:
  - `status.HighWatermark == highestCommitted`
  - `status.NextOffset == status.HighWatermark` (no uncommitted divergent suffix)
  - `status.LeaderEpoch == currentEpoch`
- **Guarded Epoch Promotion:** The controller increments `LeaderEpoch + 1` and commits the new assignment via Raft quorum before brokers apply the leadership change.
- **Replica Catch-up & ISR Admission:** Replicas outside ISR catch up in the background (`runReplicaCatchup`). A follower is admitted into the committed ISR only when its log end matches the leader's durable high watermark.

---

## Client Routing & Endpoint Resolution

The `mqclient` and CLI support dual network topologies:
- **Host / External Clients:** Clients outside Docker or Kubernetes connect via mapped host ports (e.g. `127.0.0.1:19092`). `mqclient.ResolveAddressWithBootstrap` automatically handles loopback mapping and container port rewrites.
- **Container / In-Cluster Clients:** Clients inside Docker bridge networks or Kubernetes pods connect directly via cluster DNS or container ports (e.g. `broker-1:9092`, `messlessmq-0.messlessmq-headless:9092`).
- **Dynamic Failover Discovery:** Keyed producers partition deterministically using 32-bit FNV-1a. If a broker returns `ErrorNotPartitionLeader` or `ErrorControllerAuthorityUnavailable`, the client automatically refreshes topic metadata and reroutes to the newly elected leader.

---

## Security Specifications

- **Transport Layer Security (TLS 1.2+):** Client-to-broker and inter-broker communications support optional TLS encryption with configurable CA root trust.
- **Mutual TLS (mTLS):** Enforces peer and client certificate verification (`RequireAndVerifyClientCert`). Rejects expired, untrusted, or missing client certificates.
- **Authentication & Authorization:** Client and admin TCP/HTTP connections support token-based authentication (`MESSLESSMQ_CLIENT_API_KEY`, `MESSLESSMQ_ADMIN_API_KEY`) and role-based ACLs (`role:topic:action`).
- **Path Traversal Protection:** Topic names are strictly constrained to `[a-zA-Z0-9._-]` with length 1–249. Relative paths (`..`, `.`, `/`, `\`) are rejected.
