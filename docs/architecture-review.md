# MessLessMQ — Final Architecture Review

## 1. What is now integrated into the broker executable

The current `cmd/broker` entrypoint loads validated environment configuration, parses bootstrap topics, creates a structured logger, constructs `internal/server.Server`, and runs it with SIGINT/SIGTERM cancellation.

The runtime composes these pieces for a **single standalone broker**:

- A TCP listener and the existing length-prefixed binary request protocol.
- A shared topic registry opened from a durable topic catalog at `<MESSLESSMQ_DATA_DIR>/metadata/topics.json`.
- One append-only partition log per configured topic partition under `<MESSLESSMQ_DATA_DIR>/logs`.
- PRODUCE API v1 with partition routing, persisted message keys, and `none`, `leader`, and `all` acknowledgement modes.
- CONSUME API v1 for the original value-only response and API v2 for key/value records.
- METADATA API v1 that advertises the local broker and its local partition set.
- Configured connection limits, request deadlines, IP-based token-bucket admission control, and request metrics.
- Optional TLS/mTLS on the client listener when certificate configuration is supplied.
- Optional HTTP health, readiness, diagnostics, and metrics endpoints when `MESSLESSMQ_ADMIN_ADDRESS` is configured.
- Graceful drain of in-flight operations followed by partition-log close.

The runtime integration test starts a real TCP listener, produces a keyed record with a synchronous leader acknowledgement, consumes it, retrieves metadata, stops the server, reconstructs it from the same data directory, and consumes the record again.

## 2. Runtime topology versus distributed topology

MessLessMQ now has a functioning **single-broker runtime**. The repository also contains separate educational implementations of a controller, cluster metadata model, replication manager, inter-broker TCP transport, reassignment/recovery coordinators, consumer groups, and offset store.

Those distributed packages are not yet assembled into one multi-process control/data plane. In particular:

- The runtime's metadata handler describes only the local broker and local partitions; it does not read committed metadata from a distributed controller.
- The runtime's replication manager is not connected to remote broker processes through `internal/interbroker`.
- The controller's Raft-style cluster is still an in-memory simulation, not a durable three-node consensus service.
- The broker does not dynamically register itself with a live remote controller or receive authoritative partition assignments.
- The client router is a package-level metadata cache; the CLI currently connects to the address supplied by the user and does not automatically route requests to a partition leader.
- Consumer group membership, group coordination, and committed-offset APIs are not exposed end-to-end through the public broker TCP request pipeline. The current CLI consume operation is an explicit stateless partition/offset fetch.

Therefore the three Docker Compose services and three Kubernetes StatefulSet pods are **three independently operating standalone brokers**, not a replicated three-broker cluster. They have distinct broker identities, data directories, and durable volumes, but do not replicate records between each other or fail over partition leadership.

## 3. Storage and message-format behavior

The runtime persists its topic/partition catalog atomically in `metadata/topics.json`. It opens each configured partition log on startup and relies on the storage engine's existing segment recovery.

New PRODUCE requests store key and value together in the `MLMQMSG1` envelope, which includes a CRC32 checksum. CONSUME API v2 unwraps this envelope and returns both fields. API v1 remains compatible with the older value-only response layout. Legacy raw log records are returned as values with a nil key.

The catalog is persistent, but topic creation/deletion and partition-count changes are not exposed as a live administrative API. A persisted topic's partition count is treated as immutable at startup; a mismatch in bootstrap configuration is rejected.

A produce batch is not transactional. The handler validates the routing plan before the first append, but a disk error partway through a multi-record batch can leave earlier records appended. `acks=leader` synchronizes the local partition log before acknowledging. In this standalone runtime, `acks=all` is also a local durable acknowledgement because the local broker is the entire replica set; it is **not** a distributed ISR quorum acknowledgement.

## 4. Security and operations integration

- TLS and optional mutual TLS can be enabled on the live broker listener through the environment configuration.
- Request rate limiting is active in the runtime and currently keys clients by remote IP address.
- API-key authentication and ACL authorization packages exist but are not wired into the custom binary TCP request path.
- The CLI currently supports plaintext TCP only; it has no TLS flags or client certificate loading.
- The optional HTTP admin endpoints are not authenticated by this runtime. Bind `MESSLESSMQ_ADMIN_ADDRESS` only to loopback or a trusted private management network. Do not expose it publicly.
- Metrics count requests, errors, produced messages, consume requests, and dispatch latency. Health checks inspect local partition-log offset access.
- Graceful shutdown stops accepting new connections, drains operations until the configured timeout, closes remaining client sockets, and closes topic logs.

## 5. Deployment assets

- The Docker image runs as a non-root user and writes data below `/var/lib/messlessmq`.
- Compose assigns a distinct named volume to each standalone broker.
- The Kubernetes StatefulSet uses stable pod identities and one persistent volume claim per pod. A working StorageClass/provisioner is required for PVC binding.
- The Kubernetes TCP probes only prove that the process accepts a TCP connection; they do not establish replication health or quorum readiness.

Persistent volumes protect local broker files against pod replacement where the storage provider honors the PVC. They do not make one broker's local records replicated or available from another broker.

## 6. Strengths retained from the educational phases

- Explicit TCP message framing rather than assuming TCP preserves message boundaries.
- Versioned binary protocol APIs and correlation IDs.
- Append-only segmented storage, offset/time indexes, retention, and startup recovery.
- Explicit producer acknowledgement modes and partition routing.
- Persistent message keys and CRC32 validation for runtime-produced records.
- Consumer replay and explicit offsets; standalone durable offset-store package.
- Separate replication and controller-quorum concepts, including ISR/high-watermark and term/commit-index models.
- Race-tested package implementations and repeatable unit/integration commands.

## 7. Remaining work before claiming a distributed production broker

1. Persist controller current term, vote, log, commit index, and applied metadata; recover them before elections.
2. Run a controller quorum across separate processes and carry RequestVote/AppendEntries over authenticated, TLS-protected peer connections.
3. Connect remote replication batches to the inter-broker transport, including bounded retry, timeout, epoch fencing, follower truncation, and catch-up.
4. Make partition placement authoritative and provision the correct local replica logs on each broker.
5. Route produce/fetch requests to partition leaders using refreshed cluster metadata.
6. Expose topic administration, group join/heartbeat/leave, offset commit/fetch, and group-aware consumption through versioned wire APIs.
7. Integrate API-key authentication and ACL authorization into the TCP request pipeline and provide a secure TLS-capable client.
8. Define batch retry/idempotency semantics across partial appends and server restarts.
9. Add replicated end-to-end tests for produce, follower failure, leader election, catch-up, consumer failover, and controller quorum loss.
10. Measure end-to-end throughput, p95/p99 latency, replication lag, recovery time, and durability under process, network, and disk faults.

## 8. Delivery semantics caution

The codebase includes at-most-once, at-least-once, idempotent-producer, duplicate-detection, and local effect-ledger models. The current runtime's simple PRODUCE API does not yet expose the complete idempotent producer/session lifecycle. It should not be described as a general exactly-once transaction system. Exactly-once side effects require the destination to participate in a transaction or provide idempotency/deduplication semantics.

## 9. Final assessment

MessLessMQ now has a usable, persistent, single-broker TCP runtime with produce, consume, local metadata, startup recovery, optional listener TLS/mTLS, operational instrumentation, and a CLI. The repository also contains tested educational implementations of many distributed-system components.

It is appropriate to present as a from-scratch message broker project with a functioning single-node data path and an in-progress distributed control/replication architecture. It is **not yet an autonomous replicated three-broker service or a production-ready Kafka replacement**. The next engineering milestone is to make controller quorum, broker registration, remote replication, leader routing, and failover work across real broker processes.
