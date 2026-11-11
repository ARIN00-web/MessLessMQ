# ADR 0007: Seed-based broker membership discovery

- Status: Accepted for the first multi-process cluster milestone
- Date: 2026-11-11

## Context

The broker runtime previously operated as a standalone process. The repository already contained a framed inter-broker TCP transport, but there was no runtime-level way for independent broker processes to discover one another or reject accidental cross-cluster joins.

## Decision

Add an optional `internal/membership` registry and a `MessageBrokerJoin` request handled by each broker's peer listener. Every broker has a configured cluster ID, stable broker ID, client advertised address, and peer advertised address. Joining brokers contact configured seeds, receive deterministic membership snapshots, and announce themselves to discovered peers. A data directory is pinned to one cluster ID and broker ID in an atomically written, fsynced `metadata/identity.json`.

A bootstrap broker may omit seed peers or configure itself as the only seed. A self seed is ignored by the join algorithm after configuration validation confirms that its address matches the broker's advertised peer address.

## Consequences

Positive:

- Brokers can discover the reachable membership set across separate processes.
- The cluster ID prevents accidental joins across differently named clusters.
- Broker ID/address conflicts are rejected instead of silently overwriting an existing member.
- Reusing a data directory with a different broker or cluster identity is rejected.
- METADATA responses can include known broker addresses, and the optional private admin endpoint can expose a membership snapshot.

Limitations:

- Membership snapshots are in-memory and local to each process; this is not a consensus-committed membership log.
- There is no heartbeat/lease failure detector, automatic member removal, or rejoin fencing after a broker identity conflict.
- The peer protocol is plaintext and unauthenticated. Peer listeners must stay on a trusted private network until peer TLS and authentication are implemented.
- Discovery does not provision remote partition replicas, replicate records, assign authoritative partition leaders, or implement failover.
- A joining process requires at least one reachable seed from its configured seed list; a bootstrap process has no seeds.

## Alternatives considered

- Hard-code all broker addresses into every process: rejected because broker lists must be changed manually and do not propagate automatically.
- Use the existing controller Raft simulation as a production membership source: deferred because it is in-memory and not yet running as a real cross-process quorum.
- Add an external service registry: deferred to keep the first milestone self-contained and focus on peer transport fundamentals.
