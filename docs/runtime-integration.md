# Integrated Broker Runtime

## Startup path

`cmd/broker` -> `config.LoadFromEnv` -> `server.New` -> durable topic catalog -> topic registry -> partition logs -> shared broker dispatcher -> `network.HandleConnectionWithTimeouts`. When peer membership is enabled, the server also opens the inter-broker listener, joins configured seeds, registers replication and controller RPC handlers, and then starts the optional controller runtime.

The server owns the lifetime of opened topic logs, peer listener, controller runtime, client listener, and optional admin listener. Shutdown cancels peer/controller work, stops accepting client connections, drains active operations until the configured deadline, closes remaining connections, shuts down the admin server, and closes topic logs.

## Produce path

1. Read one length-prefixed frame, decode a protocol request, and dispatch `APIPRODUCE` version 1.
2. Decode the batch and validate topic/partition routing before the first append.
3. Route each keyed message through the topic's deterministic `PartitionAssignment`; nil keys use the assignment's round-robin path.
4. Persist key/value in a CRC32-checked `MLMQMSG1` envelope and append the record timestamp.
5. `acks=none` uses the local append path; `acks=leader` syncs the local partition log; `acks=all` uses the committed controller assignment when controller mode is enabled (or the explicit static assignment in local compatibility mode), requires MinISR durable data writes, persists the leader high watermark, and requires MinISR durable commit-marker acknowledgements.
6. Return one offset result per successfully appended message.

A storage error partway through a batch can leave earlier records appended. The batch does not have transactional rollback. Without controller voters or explicit replica assignments, `acks=all` is local-only. With committed assignments, the runtime uses the peer TCP transport to query follower status, send bounded replication batches, and propagate commit markers. Error 14 means the data quorum was not reached and the leader watermark was not advanced. Error 15 means the leader persisted its watermark but commit-marker quorum confirmation is uncertain.

## Consume path

CONSUME v1 retains its original value-only response. CONSUME v2 unwraps the message envelope and returns key, value, timestamp, partition, and offsets. A stateless network request specifies an explicit topic, partition, and offset. The handler checks the local persisted exclusive high watermark and returns only records with offsets below that boundary. The richer `consumer.Consumer`, group coordinator, and offsets packages are not yet exposed as a complete wire-level consumer-group protocol.

## Metadata path

METADATA v1 describes broker addresses and partition replica/leader data. In controller mode, the view comes from the committed controller snapshot as applied locally; without controller mode, it uses local membership and configured assignment information. Clients must use returned metadata themselves. The CLI does not automatically reroute a request to a partition leader.

## Peer membership and replication

When `MESSLESSMQ_CLUSTER_ID`, `MESSLESSMQ_PEER_LISTEN_ADDRESS`, and `MESSLESSMQ_PEER_ADVERTISE_ADDRESS` are configured, the broker pins cluster and broker identity in `<DATA_DIR>/metadata/identity.json`. At startup, it opens a separate inter-broker TCP listener, registers the broker-join handler and (when configured) replication and controller handlers, and seeds its local membership registry with its own identity. `MESSLESSMQ_SEEDS` entries use `broker-id@host:port`.

A joining broker contacts a seed, supplies its cluster ID and broker identity, receives the seed's known membership snapshot, and contacts discovered peers to announce itself and learn their snapshots. A cluster ID mismatch or conflicting broker ID/address is rejected. Membership snapshots are in-memory discovery state, not committed controller metadata; there is no heartbeat/lease failure detector or membership removal protocol.

For data replication, the running server uses the assignment registry populated from committed controller metadata when controller mode is enabled. The controller leader commits broker registrations and initial partition assignments through the Raft log; followers apply only entries covered by the committed index. If `MESSLESSMQ_REPLICA_ASSIGNMENTS` is absent, every configured fixed voter is assigned as a replica and the initial leader rotates by partition ID over sorted broker IDs. An explicit ordered replica map remains supported and must be identical on every voter.

For `acks=all`, the partition leader queries assigned followers, verifies compatible log state, sends missing records, waits for durable append acknowledgements, advances its local exclusive high watermark, and sends commit markers. Followers persist the high watermark before acknowledging a commit marker. The live consume path filters uncommitted tails. The in-process `replication.Manager` remains a separate package/test model; the running server uses its `distributedAppender` transport adapter to replicate between real processes.

The committed initial assignment is authoritative, but the partition leader remains fixed until a future controller-committed reassignment with a higher leader epoch. Automatic partition leader promotion, dynamic ISR removal, staged replica reassignment, and client-side automatic rerouting are not implemented yet.

## Durable fixed-voter controller election

When `MESSLESSMQ_CONTROLLER_VOTERS` is set, each broker opens a fixed-membership Raft participant under `<DATA_DIR>/metadata/controller/raft-state.json`. The list must include the local broker ID and be identical on every voter; the persisted state rejects a changed voter set. Elections begin only after every configured voter is visible in the local membership snapshot. RequestVote and AppendEntries use the inter-broker TCP envelope.

The durable node stores current term, vote, log entries, and commit index using a temp file, file sync, atomic rename, and directory sync before reporting protocol success. Election role and known leader are volatile; on restart, the process starts as a follower and learns the current leader from AppendEntries. The controller runtime uses randomized election timeouts, majority voting, and periodic heartbeats. `GET /controller/status` reports local term/role/leader, log and commit indexes, voter IDs, and whether all configured voters are currently known to local membership. Its `ready` field indicates voter discovery only, not application health or a successful metadata commit.

The live runtime proposes broker registration and initial partition assignment commands through a serialized controller proposal path. The controller leader replicates each command to a majority, commits current-term entries, and publishes the applied committed snapshot. Followers apply committed snapshots after AppendEntries updates their commit index. The server exposes `GET /controller/metadata`, while the normal METADATA API and distributed replication adapter use the committed assignment view. A three-process smoke test verifies identical committed metadata on all brokers and a record produced through the committed leader appearing in both follower logs. Controller leader failure is tested across three processes; partition leader failure is not automatically handled by this controller yet.

## Runtime security and management

TLS/mTLS can be applied to the client broker TCP listener when configured. The current inter-broker peer listener remains plaintext and unauthenticated. Request rate limiting is applied by remote IP. The optional HTTP admin listener provides liveness, readiness, diagnostics, metrics, cluster membership, controller status, and committed controller metadata; it has no authentication middleware and must stay on loopback or a trusted management network. API-key authentication and ACL packages are not yet connected to the TCP request path.
