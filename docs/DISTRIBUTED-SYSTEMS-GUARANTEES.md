# Distributed systems guarantees

## Implemented boundaries

- Partition logs and controller state are persisted before their respective successful local operations return.
- `acks=all` commits a record only after the leader and at least `MIN_ISR - 1` current ISR followers durably append it, then durably confirm the commit marker.
- Consumers read only offsets below the local exclusive high watermark.
- The controller commits placement and ISR changes through a fixed voter majority.
- A follower enters ISR only after its leader identity/epoch, high watermark, and physical log end exactly match the observed leader state.
- A follower leaves ISR only after three consecutive bad probes; one timeout cannot lower availability.
- A stale partition leader is fenced by a short controller-authority lease and leader epoch checks.

## Explicit non-guarantees

- An error after a leader persisted a commit marker can be an uncertain producer outcome; callers must not blindly retry without idempotency.
- Peer transport and admin endpoints are not yet authenticated or encrypted.
- No automatic divergent-suffix truncation, snapshot install, reassignment, or voter-set change exists.
- Consumer-group and externally visible exactly-once semantics are not part of the current wire protocol.
