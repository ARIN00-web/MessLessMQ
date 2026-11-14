# MessLessMQ — Runtime Integration Changelog

This changelog is chronological: the initial section records the single-broker integration, and later milestone sections record the subsequent multi-process membership, replication, controller-election, and committed-metadata work. The current implementation is summarized in the final milestone section and in `docs/architecture-review.md`.

## Initial single-broker runtime milestone

### Runtime changes

- Added `internal/server` as the broker lifecycle composition root.
- Replaced the placeholder PRODUCE handler with a working API v1 implementation.
- Connected PRODUCE and CONSUME to one shared persistent topic registry.
- Added an atomic durable topic catalog at `DATA_DIR/metadata/topics.json` and startup recovery of topic partition logs.
- Added `MLMQMSG1` key/value envelopes with CRC32; CONSUME v2 returns persisted message keys while CONSUME v1 remains the value-only response format.
- Added METADATA API v1 for local standalone broker metadata.
- Added a small CLI with `produce`, `consume`, and `metadata` commands.
- Wired runtime request deadlines, connection limits, IP-based rate limiting, request metrics, optional TLS/mTLS, optional HTTP health/diagnostics/metrics, graceful drain, and topic-log close into the server lifecycle.
- Hardened network framing against empty/oversized frames and short writes.

### Deployment and operational changes

- Docker image configures a writable non-root data directory at `/var/lib/messlessmq`.
- Docker Compose uses separate named volumes and unique broker IDs for the three standalone process examples.
- Kubernetes StatefulSet uses stable pod identities and a per-pod 5Gi persistent volume claim.
- `scripts/integration.sh` includes the runtime integration package tests.
- `scripts/validate.sh` now builds broker and client binaries as well as running tests, race detection, and vet.
- `scripts/crash-restart.sh` now produces a synchronously acknowledged message, kills the broker, restarts against the same data directory, and verifies the message is still readable.
- Added runtime operations guide, ADR 0006, architecture review, and production test plan updates.

### Validation performed for the initial single-broker milestone

The uploaded `go.mod` declares Go 1.27.1. The build environment for this integration run has Go 1.23.2 and no network access to fetch the declared toolchain. For validation only, the `go` directive was temporarily changed to 1.23.2 and restored before packaging. Under Go 1.23.2:

- `go test -count=1 ./...` passed.
- `go test -race -count=1 ./...` passed.
- `go vet ./...` passed.
- `go build ./cmd/broker ./cmd/client` passed.
- `scripts/integration.sh` passed.
- `scripts/data-integrity.sh` passed.
- `scripts/crash-restart.sh` passed and verified a local sync-acknowledged record survived SIGKILL/restart.
- Bash syntax checks and YAML parsing passed.

Docker image build and Kubernetes deployment were not run in this build environment because Docker/kubectl are unavailable here. The `go.mod` file in this delivery remains set to Go 1.27.1; run the same validation on the target Go 1.27.1 toolchain.

### Limitations at the initial single-broker milestone (historical)

- Replication is not connected to remote broker processes.
- Controller Raft state is not durably persisted or run as a networked quorum.
- Each Docker/Kubernetes broker is an independent standalone instance with its own data volume.
- The local metadata handler advertises only local partitions and does not consult the cluster controller.
- Client leader routing is not wired into the CLI or request path.
- Consumer groups and durable group offsets are not exposed through the public network protocol.
- API-key authentication and ACL authorization are not wired into the TCP request pipeline. TLS/mTLS is available on the live server listener when configured; the included CLI is plaintext-only.
- Admin HTTP endpoints are optional and unauthenticated; bind them only to a trusted private interface.
- Produce batches are not transactional. `acks=all` is local-only in this single-broker runtime.
- Record CRC covers new runtime-produced key/value envelopes, not arbitrary low-level/raw storage payloads.


## Broker membership discovery milestone

- Added `internal/membership` with seed-based join, cluster-ID validation, broker-ID/address conflict detection, deterministic snapshots, and transitive peer discovery over `internal/interbroker`.
- Added optional peer listener startup and retrying seed join to `internal/server`; the client listener does not open until configured seed discovery succeeds or times out.
- Added `GET /cluster/members` to the optional admin HTTP listener.
- Added `MESSLESSMQ_CLUSTER_ID`, `MESSLESSMQ_PEER_LISTEN_ADDRESS`, `MESSLESSMQ_PEER_ADVERTISE_ADDRESS`, `MESSLESSMQ_SEEDS`, and `MESSLESSMQ_PEER_JOIN_TIMEOUT` configuration support.
- Added durable `metadata/identity.json` so a peer-enabled data directory is pinned to one cluster ID and broker ID.
- Updated Compose/Kubernetes manifests with peer listeners and seed discovery. A bootstrap process may use itself as the only seed; it ignores that self-seed after address validation.
- Added tests for three live server instances discovering all members, metadata advertising the membership list, cluster mismatch/conflicting IDs, self-seed bootstrap, and persistent identity checks.
- Added `scripts/membership-smoke.sh`, which starts three separate broker OS processes with temporary data directories and verifies all three membership views through `/cluster/members`.
- Updated `scripts/integration.sh` to include the membership and configuration packages; added local-only Compose admin port mappings and the Kubernetes admin service port for membership inspection.

This milestone is intentionally membership discovery only. It does not implement heartbeats or member removal, a durable controller quorum, remote message replication, authoritative partition placement, or automatic leader failover. The peer join transport is plaintext and unauthenticated; use a trusted private network.

## Distributed replication commit-boundary milestone

- Added a durable per-partition `replication.CommitStore` under `DATA_DIR/metadata/commits/`. Commit updates use temp-file write, file sync, atomic rename, and directory sync.
- Wired commit-store initialization into `internal/server`: standalone partitions adopt their existing log end on first initialization; assigned distributed replicas start at their retained log start until the configured leader commits a prefix.
- Connected a new inter-broker `MessageReplicationCommit` RPC. Followers validate configured leader identity/epoch and local log end before persisting a new high watermark.
- Updated `acks=all`: durable data must reach MinISR first; the leader persists its exclusive high watermark; then commit markers must reach MinISR before success is returned.
- Added explicit error code 15 (`ErrCommitUncertain`) for the case where the leader has persisted its watermark but commit-marker quorum acknowledgements are not confirmed. Error 14 remains the pre-commit insufficient-data-ISR rejection.
- Wired committed-read filtering into the runtime consume handler. Reads reject offsets beyond the high watermark, suppress records at or beyond it, and report the committed boundary as the response log end.
- Standalone appends continue to advance a local commit watermark so single-broker persistence behaviour remains intact.
- Extended `scripts/replication-smoke.sh` to prove that `acks=leader` records remain hidden until a later quorum append commits the prefix, follower commit markers arrive, MinISR=2 works with one follower unavailable, and failed quorum tails remain hidden.
- Added commit-store, monotonicity, corruption, persistence, committed-read, and runtime restart tests.
- Added ADR 0009 and updated the distributed replication guide.


## Controller-committed broker and partition metadata

The current integrated runtime now uses a serialized controller proposal path for broker registration and initial partition assignments. The elected controller leader replicates commands to a majority of the fixed voter set, commits current-term entries, and applies its committed metadata snapshot. Followers apply snapshots only after their durable Raft commit index advances.

When no explicit `MESSLESSMQ_REPLICA_ASSIGNMENTS` is configured, the server derives a deterministic full-replica assignment from sorted fixed voter IDs and rotates the initial partition leader by partition ID. Explicit ordered assignments remain supported and must be consistent with the fixed voter set. Each broker's METADATA API and replication data path consume the applied committed assignment registry. `GET /controller/metadata` exposes the committed broker and partition records.

The multi-process controller-metadata smoke test verifies three committed broker registrations and three committed partition assignments on every broker, verifies the normal client METADATA API, and produces a record through the committed leader that is consumed from each follower's local replica log. The change does not yet implement partition leader failover, staged replica reassignment, dynamic ISR removal, controller voter reconfiguration, or automatic client rerouting.


## Controller-driven partition leader failover and keyed client routing

- Added a controller-side partition failure reconciler. The elected controller performs three status probes (350 ms timeout per probe) before considering a partition leader unavailable.
- Candidate selection is restricted to the committed assignment's replica set. A candidate's commit-store leader ID and epoch must match the previous assignment; the candidate must have the highest observed high watermark and its physical log end must equal that high watermark. A candidate with an uncommitted suffix is never promoted.
- The controller commits a replacement partition leader through the existing Raft proposal path with `LeaderEpoch+1`. Every broker applies the new committed assignment and updates its local commit-store leader/epoch while preserving the high watermark.
- Added a partition-status peer RPC that is authorized against the current controller leader identity and returns local start offset, log end, high watermark, commit leader ID, and epoch.
- Added controller-authority leases. A controller leader renews its lease only after a majority heartbeat; a follower renews only after accepting AppendEntries from its known leader. Distributed appends fail closed with error code 16 when the local authority lease expires, fencing an isolated stale partition leader.
- Updated the CLI keyed `produce` path to fetch METADATA, mirror the broker's FNV-1a key partitioner, route to the committed partition leader address, and refresh metadata/retry once for `NotPartitionLeader` or `ControllerAuthorityUnavailable`. Unkeyed CLI produce and consumer fetch are still not automatically routed.
- Added tests for promotion candidate eligibility, rejecting uncommitted suffixes and stale epochs, failure threshold/backoff, authority lease expiry, network error mapping, and deterministic keyed routing.
- Added `scripts/partition-leader-failover-smoke.sh`. It starts three independent broker processes, produces a committed record, stops the partition leader, verifies committed promotion and metadata-driven rerouting, restarts the old broker from its own durable state, and verifies it catches up on a subsequent replicated append.
- Updated architecture review, runtime integration guide, production test plan, README, and ADR 0012.

Safety boundaries: failover is limited to the existing committed replica set; there is no dynamic ISR removal or staged reassignment. If no follower proves a complete committed prefix (for example, it has an uncommitted suffix), promotion is refused. The storage layer has no safe in-place suffix truncation or snapshot/rebootstrap workflow yet, so such a replica may require manual rebuild. Peer RPC and admin HTTP remain plaintext/unauthenticated and must stay on trusted private networks.

Validation on the isolated source tree used Go 1.23.2 (the build environment could not fetch the repository's Go 1.27.1 directive). The full test suite, race detector, `go vet`, broker/client builds, and membership, replication, controller-election, controller-metadata, and partition-leader-failover three-process smoke tests passed. The project's `go.mod` remains at Go 1.27.1 in the delivered overlay workflow; run the validations locally with the declared toolchain.
