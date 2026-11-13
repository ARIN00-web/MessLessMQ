# ADR 0010: Durable fixed-voter controller elections over inter-broker TCP

- Status: Accepted for Distributed Runtime v1
- Date: 2026-11-11

## Context

Broker membership discovery and synchronous, statically assigned message replication work across independent broker processes. The earlier controller implementation was an in-memory test harness; a broker restart lost its term and vote. A real controller election needs durable election state and an actual peer transport.

## Decision

Run one metadata-controller Raft participant inside each configured broker process. The voter set is explicit through `MESSLESSMQ_CONTROLLER_VOTERS` and fixed for the lifetime of the persisted controller state. The list must include the local broker ID and be identical on all voters. A changed voter set with an existing data directory is rejected.

RequestVote and AppendEntries use the existing length-prefixed inter-broker TCP envelope. `DurableNode` persists current term, voted-for, log entries, and commit index using a temporary file, file sync, atomic rename, and directory sync before protocol success is returned. Role and known leader are volatile; a restarted process begins as a follower and learns the current leader from AppendEntries.

Election timeouts are randomized within configured bounds. A node waits until all configured voter IDs are present in its local membership snapshot before campaigning. A candidate becomes controller leader only after a majority of the fixed voter set grants votes. The leader sends periodic AppendEntries heartbeats; followers reset election deadlines after accepting valid leader contact.

The optional admin endpoint `/controller/status` exposes local role, term, leader ID, log/commit indexes, voter IDs, and whether all voters are known to local membership. The `ready` field indicates discovery of the configured voter set; it does not mean that a metadata proposal has committed.

The running server currently exercises elections and heartbeat exchange. The controller node and AppendEntries handler implement durable log mechanics, but the broker runtime does not yet expose a command-proposal API for broker registration or partition assignments, and no committed controller snapshot currently drives the data-plane replica assignments.

## Consequences

- The controller can elect a replacement after a controller process stops if a majority of configured voters remain available.
- Term and vote survive process restart, preventing a restarted voter from granting a second vote in the same term.
- A restarted voter rejoins as a follower and learns the current leader through heartbeat AppendEntries.
- The fixed voter set must be identical on all brokers. Dynamic membership reconfiguration is not implemented.
- Peer RPCs currently use the plaintext peer listener. TLS and peer authentication remain required before exposing this network to untrusted environments.
- Controller election is distinct from data-partition leader election. Partition leaders remain selected by static `MESSLESSMQ_REPLICA_ASSIGNMENTS`; controller election does not promote a new partition leader, change ISR membership, or update client routing.

## Alternatives considered

1. Keep the in-memory `controller.Cluster`: rejected because it cannot survive process restart or coordinate real broker processes.
2. Elect a controller based only on membership snapshots: rejected because membership presence does not provide durable majority voting or split-brain protection.
3. Dynamically treat every discovered broker as a voter: rejected because adding voters without committed membership reconfiguration can change quorum rules inconsistently.


## Follow-up decision

The broker runtime proposal path and committed broker/partition metadata integration were added in ADR 0011. The limitations described above reflect the state when this ADR was first accepted; consult ADR 0011 and the current architecture review for the current runtime behavior. Partition leader promotion remains future work.
