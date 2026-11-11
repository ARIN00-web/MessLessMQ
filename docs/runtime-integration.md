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

METADATA v1 describes the one local broker and its configured partitions. Each partition's sole local replica and leader are the local broker ID. This is accurate for a standalone process and intentionally does not consult the in-memory cluster/controller models.

## Runtime security and management

TLS/mTLS is applied to the broker TCP listener when configured. Request rate limiting is applied by remote IP. The optional HTTP admin listener provides liveness, readiness, diagnostics, and metrics; it has no authentication middleware and must stay on loopback or a trusted management network. API-key authentication and ACL packages are not yet connected to the TCP request path.

## Distributed integration still required

The next implementation milestone is to persist controller state, run controller peers over the inter-broker transport, register brokers, provision partition replicas on their assigned hosts, send replication batches to remote processes, route clients using committed metadata, expose group/offset protocols, and prove failover with multi-process integration tests.
