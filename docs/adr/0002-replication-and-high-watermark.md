# ADR 0002: Replicate partition logs and gate committed reads by high watermark

- Status: Accepted as the Phase 14 replication model
- Scope: Partition replication

## Context

A leader must distinguish data that was appended locally from data replicated sufficiently to be treated as committed. Followers can lag or become unavailable.

## Decision

Use one leader and follower replicas per partition. Track ISR membership and a leader epoch. Advance an exclusive high watermark based on the replication progress of the required in-sync set. Committed reads are limited to offsets below that watermark.

## Consequences

- `MinISR` and acknowledgement policy are durability/availability trade-offs.
- A follower must catch up before rejoining ISR.
- Stale leader epochs must be fenced.
- The model is currently in-process; a live inter-broker replication path is still required.
- ISR and high-watermark state must be persisted/recovered consistently before production deployment.
