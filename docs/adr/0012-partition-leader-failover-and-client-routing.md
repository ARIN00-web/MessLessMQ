# ADR 0012: Controller-committed partition leader failover and keyed CLI routing

- Status: Accepted for the current distributed-runtime milestone
- Date: 2026-11-14

## Context

MessLessMQ had synchronous inter-process record replication, durable per-partition high watermarks, committed-read filtering, fixed-voter durable controller elections, and controller-committed broker/partition metadata. However, the partition leader remained fixed in the committed assignment. A failed data leader therefore stopped writes for that partition even when an assigned follower had a complete committed prefix.

A safe promotion must not elect a replica that is missing committed records or contains an uncommitted suffix. Old leaders must also stop accepting writes after losing controller authority, and producers need a way to discover the committed replacement.

## Decision

The elected controller leader periodically reconciles partition leadership. It requires three consecutive failed partition-status probes (350 ms timeout each), queries the other committed replicas, and selects only a candidate that:

1. belongs to the current committed replica list;
2. reports the current assignment's commit leader ID and leader epoch;
3. has the highest high watermark observed among valid replica reports;
4. has a physical log end exactly equal to that high watermark; and
5. has a retained start offset no greater than the high watermark.

The controller commits a replacement `CommandAssignPartition` with the same ordered replica list and `LeaderEpoch+1`. Every broker applies the committed assignment and advances its local commit-store leader identity/epoch without lowering its high watermark.

A controller-authority lease gates distributed writes. A controller leader renews it only after majority heartbeat acknowledgements; a follower renews it only after accepting AppendEntries from its current controller leader. Distributed append fails closed when the local lease is stale.

The CLI producer uses committed METADATA for keyed messages: it mirrors FNV-1a partition selection, routes to the partition leader's advertised address, and refreshes metadata/retries once after an explicit stale-leader or controller-authority error.

## Consequences

Positive:

- A partition can recover service after its leader process fails when at least one assigned follower has the complete committed prefix and the controller quorum remains available.
- A stale partition leader is fenced from writes after its controller-authority lease expires.
- Keyed CLI produce can follow a committed partition leader change without manually changing the address.
- The behavior is exercised by a three-process kill/promote/produce/restart/catch-up smoke test.

Trade-offs and limitations:

- Failure detection uses timeouts; it cannot independently distinguish a crash from a network partition. Controller quorum and the authority lease are required for safe operation.
- Promotion is limited to the existing replica list. There is no dynamic ISR membership or staged replica reassignment.
- The implementation refuses promotion if no replica proves `NextOffset == HighWatermark`. There is no in-place truncation or snapshot/rebootstrap path for divergent uncommitted suffixes.
- Keyed CLI produce is routed; unkeyed produce and consumer fetch are not automatically routed. The CLI retry is bounded to one refresh/retry.
- Peer RPCs are plaintext and unauthenticated in this milestone; peer/admin listeners must remain on a trusted private network.
- This does not make the broker production-ready by itself. Further Raft hardening, security integration, reconfiguration, and chaos testing remain necessary.
