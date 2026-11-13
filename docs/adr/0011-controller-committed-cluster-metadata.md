# ADR 0011: Commit broker registrations and partition assignments through the controller

- Status: Accepted
- Date: 2026-11-12

## Context

The broker runtime already had seed-based membership, synchronous inter-broker replication, durable per-replica commit markers, and a fixed-voter durable controller election. However, the data plane still consumed replica assignments directly from environment configuration. That meant the controller election and the data-plane cluster view were separate mechanisms: the elected controller could not authoritatively publish the broker catalog or initial partition placement.

## Decision

Use the fixed-voter controller's Raft log as the source of truth for broker registrations and initial topic-partition assignments.

The elected controller leader proposes `RegisterBroker` and `AssignPartition` commands. It persists and replicates each proposal to the fixed voter set, requires a majority, and commits current-term entries. Each broker applies only the prefix covered by its durable commit index. The resulting applied snapshot feeds:

- `GET /controller/metadata`;
- the normal METADATA API;
- the live data-plane replica assignment registry used by produce replication and follower authorization.

When no explicit replica map is supplied, bootstrap derives a deterministic assignment using sorted fixed voter IDs. Every fixed voter is a replica, and the initial leader rotates by partition ID. An explicit ordered assignment map remains supported, but each replica must be a member of the configured voter set and all brokers must use the same configuration.

Startup reconciliation may create missing initial assignments. It never silently overwrites an existing committed assignment.

## Consequences

### Positive

- Broker registration and initial partition placement are committed through the controller quorum rather than independently trusted on each broker.
- METADATA and replication routing consume a single applied assignment view.
- A new broker does not serve controller-backed produce until committed metadata supplies its partition assignment.
- The same controller metadata is inspectable on each broker through a dedicated admin endpoint.

### Trade-offs and limitations

- The voter set is fixed and must be configured identically on all voters.
- The default placement policy is deterministic, full-replica, and not rack/load/capacity aware.
- Existing assignments are immutable to startup reconciliation; staged reassignment is a separate future workflow.
- Partition leader promotion and dynamic ISR membership are not implemented by this decision.
- The live Raft transport still needs per-peer replication progress/backtracking, snapshot transfer, and voter reconfiguration before large-scale or production use.
- Peer RPC and the admin HTTP listener remain plaintext/unauthenticated and must stay on trusted private networks.
