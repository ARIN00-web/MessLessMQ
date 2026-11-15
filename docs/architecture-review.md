# MessLessMQ — Final Architecture Review

**Reviewed against the current source tree:** 2026-11-15

## 1. Integrated broker runtime

`cmd/broker` starts a persistent broker with a versioned, length-prefixed TCP protocol. The runtime integrates topic catalogs, append-only partition logs, local commit stores, the data-plane dispatcher, client authentication/ACL enforcement, optional client TLS, an optional inter-broker listener, membership discovery, and a fixed-voter durable controller runtime.

Implemented runtime features:

- PRODUCE v1 and v2; v2 accepts an explicit partition selected by the client.
- CONSUME v1 and v2; v2 returns keys, values, timestamps and offsets.
- METADATA v1; controller mode exposes committed broker and partition assignments.
- Persistent topic catalog and append-only segmented partition storage, indexes, retention and recovery.
- Per-partition durable high-watermark/leader-epoch commit state.
- Synchronous inter-process record replication and commit-marker propagation.
- MinISR enforcement using controller-committed ISR membership.
- Fixed-voter controller election with durable term, vote, log and commit index.
- Controller-committed broker registration, partition assignment, ISR updates and staged reassignment intent.
- Safe in-set partition leader promotion under a higher leader epoch and controller-authority lease fencing.
- Conservative ISR reconciliation: a replica is admitted only after exact log-end/high-watermark compatibility; an ISR member is removed after three consecutive bad probes.
- Background catch-up for assigned replicas outside ISR; controller reconciliation, not the copier, decides when a replica becomes in-sync.
- Stage/finalize replica reassignment through authenticated-optional admin endpoints. New targets are first placed in a committed union assignment, copied, admitted to ISR, and only then finalized.
- Reusable `mqclient` producer/consumer routing with per-topic metadata caching, keyed FNV-1a partition selection, unkeyed round-robin selection, bounded stale-route retry, optional TLS/mTLS client certificates and optional API-key AUTH.
- GROUP v1 wire handlers for join, heartbeat, leave, owned-partition fetch, durable offset commit and offset fetch.
- Optional client TLS/mTLS, peer TLS/mTLS, binary API-key authentication, topic ACLs, admin Bearer-key protection, rate limiting and graceful shutdown.
- Health, diagnostics, Prometheus metrics, Docker Compose and Kubernetes manifests.

## 2. Data replication and ISR

`acks=all` requires the leader and at least `MIN_ISR - 1` replicas from the currently committed ISR to durably append the record. The leader then persists its exclusive high watermark and obtains durable commit-marker acknowledgements from the required ISR members. Consumers hide offsets at or beyond their local high watermark.

A failed quorum may leave an uncommitted physical suffix. It remains hidden from committed reads. The system does not automatically truncate a divergent suffix. A failure after the leader persists the commit marker but before enough follower marker acknowledgements is an uncertain producer outcome; callers must not blindly retry.

The controller ISR reconciler probes the leader and replicas. It expands ISR only when leader identity, epoch, physical log end and durable high watermark match. It tolerates brief failures for existing ISR members and removes them only after three consecutive failed/incompatible probes. A new replica is copied by the leader's catch-up loop before the controller admits it to ISR.

### Staged reassignment

The live server has stage and finalize operations. Staging commits the union of current and target replicas and stores `ReassignmentTarget`; the current ISR remains the acknowledgement set. Background catch-up transfers records to targets outside ISR. Finalization rejects targets not yet in committed ISR, commits the target replica set, clears the staged target and increments the leader epoch.

The new `scripts/reassignment-smoke.sh` starts three independent brokers, removes one replica, appends another committed record, adds the replica back, waits for catch-up and ISR admission, finalizes, and verifies both records are readable from the re-added broker.

Limitations: there is no automatic truncation or snapshot rebootstrap for a divergent target, no rack-aware placement planner, and no autonomous policy engine to select reassignment targets.

## 3. Controller consensus

The controller is a fixed-voter Raft-style implementation. The configured voter IDs must match on every broker and remain unchanged across restarts. Term, vote, complete log and commit index are durably persisted. A current-term entry must be replicated to a majority before commit. Followers reject mismatched prefixes and never overwrite committed entries. Higher terms force a leader to step down. Authority leases fail closed when quorum contact is lost.

The runtime now maintains per-follower `nextIndex` and `matchIndex`, sends incremental AppendEntries suffixes, and backs `nextIndex` up using follower conflict hints. This avoids retransmitting the entire log for every heartbeat/proposal and supports follower catch-up from retained log index 1.

**Not implemented:** snapshots, snapshot installation, log compaction, retained-log base indexes, and dynamic voter reconfiguration. Since the entire controller log is retained and followers must catch up from index 1, metadata-log growth is currently unbounded. These features require a coordinated durable-state format and crash-recovery design; do not add an unsafe shortcut or claim production-grade Raft without them.

## 4. Client and consumer groups

The `mqclient` package supports metadata discovery, keyed and unkeyed produce routing, consumer fetch routing, metadata TTL, bounded retries, context deadlines, TLS/mTLS and API-key authentication. It retries only stale leader/authority responses; it does not retry uncertain commit outcomes or arbitrary transport errors after a possible append.

The CLI supports the same TLS and AUTH settings through environment variables:

- `MESSLESSMQ_CLIENT_API_KEY`
- `MESSLESSMQ_TLS_CA_FILE`
- `MESSLESSMQ_TLS_CLIENT_CERT_FILE`
- `MESSLESSMQ_TLS_CLIENT_KEY_FILE`
- `MESSLESSMQ_TLS_SERVER_NAME`

GROUP v1 is integrated into the broker dispatcher and supports join, heartbeat, leave, owned-partition fetch, durable offset commit and offset fetch. However, group membership and assignments are local to one broker process and in-memory. Offset commits are durable on that broker, but coordinator state is not controller-replicated. Coordinator-broker loss requires members to rejoin and recover offsets. There is no distributed group coordinator discovery/forwarding or static membership protocol. Do not claim cross-broker exactly-once processing.

## 5. Security

Client listener TLS/mTLS is available through `TLS_CERT_FILE`, `TLS_KEY_FILE`, `TLS_CLIENT_CA_FILE` and `REQUIRE_CLIENT_CERT`. Peer TLS/mTLS is available through `PEER_TLS_CERT_FILE`, `PEER_TLS_KEY_FILE`, `PEER_TLS_CA_FILE` and `REQUIRE_PEER_CERT`. Peer TLS authenticates certificates against the configured CA and verifies advertised peer hostnames, but certificate subjects/SAN identities are not yet bound to broker IDs at the application layer.

`CLIENT_API_KEY` requires AUTH v1 before regular requests. `CLIENT_ACL` maps roles to topic/action permissions. The current deployment has one configured client key and one role; it is not a full multi-principal credential-management system. `ADMIN_API_KEY` protects administrative HTTP routes while keeping liveness/readiness probes available.

Security remains opt-in for backward compatibility. The default Compose and Kubernetes lab manifests are development configurations and do not provision certificates or API keys. Keep peer and admin traffic on trusted networks until you configure TLS/mTLS, admin authentication and suitable network policy. Certificate rotation automation and credential reload are not implemented.

## 6. Deployment

Docker Compose starts three brokers with independent named volumes, seed discovery, a fixed three-voter controller set and MinISR=2. Kubernetes uses a three-replica StatefulSet, one PVC per broker, a headless peer-discovery service and a client ClusterIP service. Kubernetes readiness/liveness probes use the admin health endpoints; readiness currently checks registered local health checks and does not prove controller quorum or replicated-cluster readiness.

Kubernetes manifests are templates, not a managed production installation. Operators must build/load the matching image, verify storage-class and PVC behavior, provide secrets/certificates, set resource limits and network policies appropriate to their environment, and validate restart/upgrade procedures.

## 7. Known limitations and production risks

1. No controller snapshots/log compaction; Raft log grows without bound.
2. Fixed voter set only; safe voter reconfiguration is not implemented.
3. No automatic rebootstrap/truncation of divergent data replicas.
4. Consumer group membership/coordinator state is broker-local and volatile.
5. Topic create/delete/partition expansion is not yet a fully coordinated live admin API.
6. Peer certificate identity is not mapped to the claimed broker ID; use private networks and trusted certificates.
7. Client authentication supports one configured API key/role per broker process; no credential rotation/reload.
8. Client TLS and AUTH are supported by the reusable Go client and CLI, but application-specific TLS deployment still requires certificate provisioning.
9. Producer outcome can be uncertain after commit-marker persistence or a lost response; no complete idempotent producer session/transaction API exists.
10. Network partition, disk-full, corruption, long-duration soak, and production-scale load/SLO testing remain necessary.

## 8. Validation evidence

The current source was tested in the packaging environment using Go 1.23.2 after temporarily changing only the local `go` directive for compatibility; the repository's declared Go 1.27.1 directive was restored for release. The following passed under Go 1.23.2:

- `go test -count=1 ./...`
- `go test -race -count=1 ./...`
- `go vet ./...`
- `go build ./cmd/broker ./cmd/client`
- `scripts/integration.sh`
- `scripts/data-integrity.sh`
- `scripts/membership-smoke.sh`
- `scripts/replication-smoke.sh`
- `scripts/controller-election-smoke.sh`
- `scripts/controller-metadata-smoke.sh`
- `scripts/partition-leader-failover-smoke.sh`
- `scripts/reassignment-smoke.sh`
- `scripts/crash-restart.sh`

The exact Go 1.27.1 toolchain was not available in the packaging environment, and Docker/Kubernetes deployment was not executed there. Do not treat Go 1.23.2 validation as proof of the declared toolchain build.

## 9. Assessment

MessLessMQ is a substantial, multi-process distributed-systems portfolio project with persistent storage, replication, durable controller metadata, failover, ISR tracking, staged reassignment, client routing, and a versioned consumer-group API. It is suitable for portfolio demonstration and controlled experimentation on a trusted network. It is **not yet production-ready** for critical workloads until Raft snapshot/recovery, voter changes, group coordinator recovery, divergent replica rebootstrap, peer identity authorization, secret rotation, and broader fault/soak testing are addressed.
