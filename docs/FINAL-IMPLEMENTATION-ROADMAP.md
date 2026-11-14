# Final implementation roadmap

## Audit status — 2026-11-14

The working runtime is a persistent TCP broker with durable topic logs, versioned PRODUCE/CONSUME/METADATA APIs, controller-committed static placement, synchronous inter-broker replication, durable high-watermark markers, fixed-voter controller election, and safe in-set partition-leader failover. The active paths are `internal/server`, `internal/broker`, `internal/interbroker`, `internal/controller`, `internal/replication`, and `internal/storage`.

The audit found that `internal/replication.Manager` had dynamic ISR behavior, while the multi-process server used only static assignments. This was an integration gap, not merely a missing package.

## Completed in this pass

- Added ISR to controller partition metadata and durable Raft commands.
- Preserved compatibility with older durable assignments by treating an omitted ISR as the full replica set.
- Made the live appender replicate and count acknowledgements from the currently committed ISR.
- Added controller-leader ISR reconciliation: exact log/high-watermark compatibility expands ISR; three consecutive failed or incompatible probes contract it.
- Added focused safety tests for catch-up compatibility and transient-failure protection.

## Next implementation stages

1. Reassignment: durable staged-plan persistence and restart restoration are complete in `internal/recovery`; next, bootstrap a new copy from an authoritative live leader and expose the controller-coordinated workflow through the broker runtime.
2. Client library: completed reusable producer/consumer APIs with metadata TTL, round-robin unkeyed placement, bounded retry and explicit uncertain-append errors. The CLI producer uses it; next, add TLS connection support.
3. Consumer groups: versioned join, heartbeat, leave, offset commit/fetch APIs and durable coordinator state.
4. Security: TLS/mTLS and identity verification for peer transport, then request-pipeline authentication and ACL enforcement.
5. Controller hardening: per-follower replication progress/backtracking, snapshot installation/compaction, and deterministic partition fault tests.
6. Operations: controller APIs for topic/reassignment status, peer-authenticated admin controls, lag/ISR metrics, and deployment security defaults.

## Risks and acceptance criteria

The ISR reconciler intentionally fails closed: a replica is admitted only when its log end and durable high watermark exactly equal the leader's observed state. It never truncates a divergent suffix. This protects committed data, but a divergent or retention-behind replica still needs rebootstrap.

Before controlled staging, the next stages must provide authenticated peer transport, rebootstrap/reassignment recovery, reusable clients, and network/disk fault coverage. A production claim additionally requires measured load behavior, secure secret rotation, and a full Raft safety review.
