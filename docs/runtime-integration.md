# Integrated Broker Runtime

## Startup path

`cmd/broker` -> `config.LoadFromEnv` -> `server.New` -> durable topic catalog -> topic registry -> partition logs -> shared broker dispatcher -> `network.HandleConnectionWithTimeouts`.

The server owns the lifetime of all opened topic logs. On shutdown it stops accepting new connections, begins operation drain, closes active connections after the drain deadline, waits for connection goroutines, shuts down the optional HTTP admin server, then closes topic logs.

## Produce path

1. Read one length-prefixed frame, decode a protocol request, and dispatch `APIPRODUCE` version 1.
2. Decode the batch and validate topic/partition routing before the first append.
3. Route each keyed message through the topic's deterministic `PartitionAssignment`; nil keys use the assignment's round-robin path.
4. Persist key/value in a CRC32-checked `MLMQMSG1` envelope and append the record timestamp.
5. `acks=none` uses append without an explicit sync; `acks=leader` and `acks=all` sync the local partition log before the response.
6. Return one offset result per successfully appended message.

A storage error partway through a batch can leave earlier records appended. The batch does not have transactional rollback. In this standalone runtime, `acks=all` is not a remote ISR quorum acknowledgement.

## Consume path

CONSUME v1 retains its original value-only response. CONSUME v2 unwraps the message envelope and returns key, value, timestamp, partition, and offsets. A stateless network request specifies an explicit topic, partition, and offset. The richer `consumer.Consumer`, group coordinator, and offsets packages are not yet exposed as a complete wire-level consumer-group protocol.

## Metadata path

METADATA v1 describes the local broker and its configured partitions. If peer membership is enabled, its broker list includes the discovered broker addresses from this process's membership snapshot. Partition assignments remain local: each partition's sole replica and leader are the broker ID of the process that opened that partition log. The handler intentionally does not consult committed metadata from the in-memory controller model.

## Runtime security and management

TLS/mTLS is applied to the broker TCP listener when configured. Request rate limiting is applied by remote IP. The optional HTTP admin listener provides liveness, readiness, diagnostics, and metrics; it has no authentication middleware and must stay on loopback or a trusted management network. API-key authentication and ACL packages are not yet connected to the TCP request path.

## Distributed integration still required

The next implementation milestone is to persist controller state, run controller peers over the inter-broker transport, register brokers, provision partition replicas on their assigned hosts, send replication batches to remote processes, route clients using committed metadata, expose group/offset protocols, and prove failover with multi-process integration tests.


## Peer membership discovery

When `MESSLESSMQ_CLUSTER_ID`, `MESSLESSMQ_PEER_LISTEN_ADDRESS`, and `MESSLESSMQ_PEER_ADVERTISE_ADDRESS` are configured, `server.New` first verifies or atomically creates the data directory's persisted broker identity. At `Run`, the runtime starts a separate inter-broker TCP listener before it opens the client listener. It registers a `MessageBrokerJoin` handler and seeds the local in-memory membership registry with the broker's own identity. Configured `MESSLESSMQ_SEEDS` entries use `broker-id@host:port`.

A joining broker contacts a seed, supplies its cluster ID and broker identity, receives the seed's known membership snapshot, and then contacts discovered peers to announce itself and learn their snapshots. A cluster ID mismatch, an unknown seed identity, or a conflicting broker ID/address is rejected. A bootstrap broker can omit seeds or specify itself as its only seed; the latter is ignored after configuration validation.

The membership identity is pinned to `<DATA_DIR>/metadata/identity.json` using an atomic write and fsync. It contains the cluster ID and broker ID, not network addresses, so service DNS/address changes do not silently change durable broker identity. A data directory cannot be reused under a different cluster ID or broker ID.

With an admin listener configured, `GET /cluster/members` returns the process's current membership snapshot. The endpoint is unauthenticated like the other admin endpoints and must remain on a trusted management network.

This milestone establishes broker discovery only. Membership is in-memory, has no heartbeat/lease-based failure detection or removal, and is not replicated through a controller quorum. The peer listener is plaintext and unauthenticated. METADATA can list discovered broker addresses, but each topic partition remains local to the process that opened its log, with that process as its sole replica and leader. There is no remote message replication, shared partition assignment, or automatic failover yet.
