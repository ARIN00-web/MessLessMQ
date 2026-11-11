# ADR 0003: Use a Raft-style replicated metadata log

- Status: Accepted as an educational controller model
- Scope: Cluster metadata

## Context

Broker registration, replica assignments, and leader metadata need one agreed order. Independent updates from multiple controllers can produce conflicting cluster views.

## Decision

Model a fixed-membership controller quorum with terms, one vote per term, candidate log freshness checks, AppendEntries log matching, majority commit, and application of committed commands.

## Consequences

- A minority partition cannot commit metadata.
- A newer term fences older leaders.
- Uncommitted conflicting suffixes may be replaced; committed entries may not be overwritten.
- The current controller model is in-memory and deterministic for tests.
- Durable Raft state, real network RPC, membership changes, snapshots, and operational election timeouts remain production work.
