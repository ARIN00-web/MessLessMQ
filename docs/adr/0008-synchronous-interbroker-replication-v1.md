# ADR 0008: Synchronous inter-broker replication v1

- Status: Accepted for the first distributed replication milestone
- Date: 2026-11-11

## Context

MessLessMQ already had a transport-neutral replication batch codec, a separate inter-broker TCP transport, and a broker membership registry. The runtime still appended PRODUCE requests only to the local partition log. The first distributed step needs to connect those existing pieces without claiming that controller elections or failover are complete.

## Decision

Use an explicit, identical static replica assignment on every broker. Each assignment is written as `topic:partition=leader,follower,...`; the first ID is the fixed leader for epoch 1. The server registers replication status and replication batch handlers on the existing private peer listener.

For `acks=all`, the leader serializes appends per partition, appends and syncs the local record, queries each follower's log range, verifies the latest common record, sends bounded encoded batches, and counts only follower responses received after their local `AppendAtSync` completes. The append succeeds when the leader plus successful followers meet `MinISR`.

Followers only accept replication from the configured leader for the partition, only when their own broker ID is listed as a replica, and only when the configured leader epoch matches. Repeated identical offsets are idempotent; divergent records and offset gaps are rejected.

## Consequences

Positive:
- Real independent broker processes can replicate bytes over TCP.
- `acks=all` has a concrete synchronous persistence requirement.
- `MinISR` is enforced and tested under follower failures.
- Existing record storage, key envelope, and replication batch encoding are reused.

Negative / deferred:
- Replica placement and leader identity are static configuration, not controller-committed metadata.
- No durable high watermark, commit marker, or committed-read filter exists yet.
- No automatic leader failover, ISR membership protocol, or snapshot bootstrap exists.
- `acks=leader` does not trigger background replication in this version.
- Inter-broker traffic remains plaintext and unauthenticated.
- The server-side transport adapter is separate from the in-process replication.Manager, which still assumes all replica partitions are in one process.

## Follow-up

Persist replication progress and commit watermarks, gate reads on committed offsets, connect controller metadata to replica assignment/epoch changes, then implement leader failover and recovery.
