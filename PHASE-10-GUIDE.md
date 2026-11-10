# MessLessMQ — Phase 10: Consumer Groups

This overlay adds a new `internal/group` package. Extract it at the MessLessMQ repository root. It does not replace Phase 9 consumer, protocol, broker, network, or storage files.

## Locked curriculum coverage

- 10.1 Why Consumer Groups
- 10.2 Group Membership
- 10.3 Partition Ownership
- 10.4 Group Coordinator
- 10.5 Rebalancing
- 10.6 Consumer Failure
- 10.7 Group Testing

## Design

A consumer group is a set of consumers that cooperate to process one or more topics. Within one group, every partition of each subscribed topic is assigned to at most one member at a time. Different groups can independently read the same topic partitions.

The coordinator maintains in-memory group membership, topic subscriptions, assignment snapshots, a monotonically increasing generation, and each member's last heartbeat. It performs deterministic eager rebalancing:

1. Sort subscribed topic names.
2. For each topic, collect and sort the members subscribed to that topic.
3. Sort that topic's partitions by ID.
4. Assign partitions round-robin over the sorted eligible members.
5. Build replacement `consumer.Consumer` instances before publishing the new assignments.
6. Increment the group generation and mark the group stable.

The ordering is deterministic, but this is an eager strategy: membership changes may revoke and reassign multiple partitions, even when some owners could have stayed unchanged.

## Main API

```go
coordinator, err := group.NewCoordinator(registry)

joined, err := coordinator.JoinGroup(
    "order-workers", "worker-a", []string{"orders"},
)

result, err := coordinator.Fetch(
    "order-workers",
    "worker-a",
    joined.Generation,
    "orders",
    0,
    100,
)

err = coordinator.Heartbeat(
    "order-workers", "worker-a", joined.Generation,
)

description, err := coordinator.DescribeGroup("order-workers")

newGeneration, err := coordinator.Rebalance("order-workers")

newGeneration, err = coordinator.LeaveGroup("order-workers", "worker-a")

expired, err := coordinator.ExpireMembers(time.Now(), 30*time.Second)
```

## Subchapter explanations

### 10.1 — Why Consumer Groups

Multiple consumers in one group divide a topic's partitions and process them in parallel. A partition is assigned to only one member in the group at a time. Two different groups can each read the same partition independently, for example an analytics group and a notifications group.

### 10.2 — Group Membership

A member joins with a group ID, a unique member ID within that group, and one or more topic subscriptions. Membership changes advance the generation. `LeaveGroup` removes a member. Duplicate member IDs and missing topic subscriptions are rejected.

### 10.3 — Partition Ownership

Ownership is represented as `(topic, partitionID)`. Assignment is topic-specific: only members subscribed to a topic are eligible to own its partitions. The coordinator ensures that no logical topic-partition is assigned to two members in the same group. Ownership is independent across groups.

### 10.4 — Group Coordinator

`Coordinator` is the single in-memory authority for group membership, generation, assignment, heartbeat, and fetch authorization. All state changes are serialized by its mutex. The generation is included in fetch and heartbeat operations to fence stale members after rebalances.

### 10.5 — Rebalancing

Joining, leaving, explicit `Rebalance`, and successful member expiration cause a new assignment generation. This implementation uses deterministic eager round-robin assignment. It preserves a member's in-memory cursor if that same member retains the same partition. A partition moved to another member starts at its earliest retained offset because committed offsets are Phase 11.

### 10.6 — Consumer Failure

Members refresh liveness with `Heartbeat`. The caller periodically invokes `ExpireMembers(now, timeout)`; members whose last heartbeat is older than the timeout are removed and the group rebalances. This package does not launch its own background timer. Fetch requests with old generations fail with `ErrStaleGeneration`, preventing an old member from continuing to fetch after ownership changes.

### 10.7 — Group Testing

Tests cover membership validation, deterministic assignments, topic-specific subscriptions, multiple subscriptions, ownership uniqueness, independent groups, leaving, generations, heartbeat, expiration, fenced fetches, retained positions, zero-assignment members, descriptions, and concurrent joins.

## Important limitations (intentional phase boundaries)

- Group state and member heartbeats are in-memory; persistence and recovery are not implemented here.
- Consumer offsets are in-memory read positions. Durable offset commits belong to Phase 11.
- This is eager, deterministic rebalancing, not Kafka's cooperative-sticky protocol.
- A moved partition starts from its earliest retained offset until Phase 11 provides committed offsets. Reprocessing/duplicates are therefore possible after reassignment.
- The caller must schedule `ExpireMembers` periodically; the group package does not own a goroutine or timer.
- No network group protocol is added in Phase 10; the group package exposes a coordinator API for broker integration in later work.
- Partition expansion requires an explicit `Rebalance(groupID)` to recompute ownership.

## Validation

From the repository root:

```bash
gofmt -w internal/group/
go test -count=1 ./...
go test -race -count=1 ./...
go test -cover ./internal/group
go vet ./...
```

Do not mark Phase 10 complete until the full tests, race tests, and vet pass on the local repository.
