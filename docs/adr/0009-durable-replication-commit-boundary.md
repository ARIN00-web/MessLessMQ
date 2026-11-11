# ADR 0009: Durable replication commit boundary and committed reads

- Status: Accepted for distributed replication v1.1
- Date: 2026-11-11

## Context

Distributed replication v1 copied and synced records to followers over the inter-broker TCP transport and enforced MinISR for data acknowledgements. However, it did not persist a commit boundary. A follower could therefore contain a durable but uncommitted tail, and the consume handler previously exposed the physical log end.

Before leader failover can be considered, the runtime needs a durable, monotonic indication of which prefix is committed and a read path that never exposes records beyond it.

## Decision

Persist one commit state per local topic-partition replica under `DATA_DIR/metadata/commits/`. Each state records the topic, partition ID, fixed leader ID, leader epoch, version, and exclusive high watermark.

The store updates state through atomic file replacement: write a temporary file, sync it, close it, rename it over the prior state, and sync the containing directory. It rejects watermark regression, an update beyond the local physical log end, and a leader/epoch mismatch.

For assigned replicated partitions, `acks=none` and `acks=leader` do not advance the high watermark. For `acks=all`, the leader:

1. Appends and syncs the record locally.
2. Catches assigned followers up through the leader log end and waits for durable follower append acknowledgements.
3. Requires the leader plus followers to satisfy MinISR.
4. Persists the leader's new exclusive high watermark.
5. Sends a commit-marker RPC to the followers that acknowledged the data and requires MinISR commit-marker acknowledgements before returning success.

A follower accepts a commit marker only from the configured leader/epoch and only if its local log end is at least the requested watermark. It persists the watermark before acknowledging.

The consume handler obtains the local committed boundary, rejects requested offsets above it, filters records at or beyond it, and reports the committed boundary as the response log end. In standalone mode, local synchronous appends advance the local watermark so existing single-broker semantics remain useful.

If a data quorum is not reached before the leader watermark advances, return insufficient ISR (error 14) and leave the watermark unchanged. If the leader watermark has advanced but commit-marker acknowledgements fail to reach MinISR, return an explicit uncertain-outcome error (code 15); do not roll back a persisted watermark.

## Consequences

Positive:
- Commit boundaries survive broker process restarts.
- Consumers cannot observe local or follower uncommitted tails.
- A later successful quorum append can replicate and commit earlier leader-only records as a contiguous prefix.
- Stale epochs, watermark regression, and watermarks beyond local log end are rejected.
- The replication smoke test exercises leader-only pending data, commit-marker propagation, follower reads, and insufficient-ISR visibility.

Negative / deferred:
- Commit state is local to each broker; no controller coordinates commit reconciliation after leader loss.
- Static leader ID and epoch 1 remain configured per process; no election or automatic promotion exists.
- Error 15 is an intentionally ambiguous producer result. The append may already be committed on the leader and some followers, so retry can create a duplicate without an end-to-end idempotent producer session.
- A failed data quorum may leave an uncommitted physical tail. Consumers hide it, but truncation/reconciliation is not yet implemented.
- Controller Raft state is not yet durable or networked across processes.

## Follow-up

Persist and run the controller quorum across processes, make partition assignments/epochs authoritative, reconcile replica tails against the committed prefix, then implement leader promotion constrained by durable high-watermark evidence.
