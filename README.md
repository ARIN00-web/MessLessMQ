# MessLessMQ

MessLessMQ is a from-scratch Go message broker project. Each broker process provides a persistent TCP data path with durable topics/partitions, PRODUCE, CONSUME, and METADATA APIs, a small CLI, optional TLS/mTLS, metrics, health endpoints, request limits, and graceful shutdown. An optional peer listener supports seed-based broker membership discovery and a fixed-voter, durable Raft controller election across independent processes.

The repository integrates replication, fixed-voter controller consensus, committed cluster metadata, inter-broker transport, consumer groups, durable offsets, recovery, conservative controller-committed ISR tracking, background replica catch-up, and staged replica reassignment. The controller promotes only an eligible in-set partition replica under a higher leader epoch after repeated leader-probe failures. The reusable Go client and CLI route keyed and unkeyed production and consumer fetches using cached metadata, with bounded stale-route refresh. The client supports optional TLS/mTLS and API-key AUTH. Divergent replica rebootstrap, Raft snapshots/compaction, dynamic voter reconfiguration, and distributed consumer-group coordinator recovery remain unfinished.

See [docs/architecture-review.md](docs/architecture-review.md) for the implemented boundary and remaining distributed-runtime work.

## Requirements

- Go 1.27.1 (as declared in `go.mod`)
- macOS, Linux, or another supported Go platform

## Run a local broker

```bash
MESSLESSMQ_BROKER_ID=local-1 \
MESSLESSMQ_LISTEN_ADDRESS=127.0.0.1:9092 \
MESSLESSMQ_ADVERTISE_ADDRESS=127.0.0.1:9092 \
MESSLESSMQ_DATA_DIR=./data \
MESSLESSMQ_BOOTSTRAP_TOPICS=orders:3,payments:1 \
go run ./cmd/broker
```

The topic catalog is persisted under `./data/metadata/topics.json`; partition logs are stored under `./data/logs`. Reuse the same data directory to recover topics and records after restart. A topic's partition count is fixed by the persisted catalog; a conflicting bootstrap definition is rejected.

## Use the CLI

In another terminal:

```bash
go run ./cmd/client metadata -addr 127.0.0.1:9092 -topic orders
```

```bash
go run ./cmd/client produce -addr 127.0.0.1:9092 -topic orders -key customer-7 -value order-created -acks leader
```

```bash
go run ./cmd/client consume -addr 127.0.0.1:9092 -topic orders -partition 0 -offset 0 -max 10
```

The CLI producer automatically fetches METADATA and routes records to the committed partition leader. It mirrors the broker's FNV-1a key partitioner for keyed records and uses client-side round robin plus PRODUCE v2 for unkeyed records. It refreshes metadata once after a stale-leader/authority response. Set `-auto-route=false` for direct diagnostic writes.

## API versions

| API | Version | Behavior |
|---|---:|---|
| PRODUCE | 1 | Backward-compatible broker-side partition selection |
| PRODUCE | 2 | Explicit client-selected partition; one message per request |
| CONSUME | 1 | Legacy value-only response |
| CONSUME | 2 | Key/value response with timestamp and offsets |
| METADATA | 1 | Broker and partition metadata applied from the committed controller snapshot when controller mode is enabled; otherwise local membership/configured assignment view |
| GROUP | 1 | Join, heartbeat, leave, owned-partition fetch, durable offset commit, and offset fetch |

`acks=leader` synchronizes the local log before acknowledging. In controller mode, partition assignments are first committed through the fixed-voter Raft log and then used by the live replication path. `acks=all` waits for the assigned MinISR data writes, persists the leader high watermark, and requires MinISR durable commit-marker acknowledgements. Without controller voters or static replica assignments, the broker operates local-only. A produce batch is not transactional if a storage error occurs after earlier records in the batch were appended.

## Configuration

The broker reads environment variables prefixed with `MESSLESSMQ_`:

| Variable | Default / purpose |
|---|---|
| `BROKER_ID` | `broker-1` |
| `LISTEN_ADDRESS` | `127.0.0.1:9092` |
| `ADVERTISE_ADDRESS` | `127.0.0.1:9092`; follows an overridden listen address if not explicitly set |
| `DATA_DIR` | `./data` |
| `BOOTSTRAP_TOPICS` | Empty; comma-separated `name:partition-count` entries |
| `REPLICA_ASSIGNMENTS` | Optional static map such as `orders:0=broker-1,broker-2,broker-3;orders:1=broker-2,broker-3,broker-1` |
| `MIN_ISR` | Minimum number of replicas required for `acks=all`; defaults to `1` (the three-broker deployment manifests set it to `2`) |
| `LOG_LEVEL` | `info` |
| `MAX_CONNECTIONS` | `10000` |
| `REQUEST_TIMEOUT` | `10s` |
| `SHUTDOWN_TIMEOUT` | `30s` |
| `RATE_LIMIT_PER_SECOND` | `1000` per remote IP |
| `RATE_LIMIT_BURST` | `2000` per remote IP |
| `CLUSTER_ID` | Optional shared cluster identity; enables peer membership when peer addresses are configured |
| `PEER_LISTEN_ADDRESS` | Optional local TCP listener for peer membership, e.g. `:19093` |
| `PEER_ADVERTISE_ADDRESS` | Optional reachable peer address advertised to other brokers |
| `SEEDS` | Optional comma-separated `broker-id@host:port` seed peers |
| `PEER_JOIN_TIMEOUT` | Maximum time to discover/join configured seeds; default `30s` |
| `CONTROLLER_VOTERS` | Optional fixed comma-separated Raft voter IDs; must include this broker and match on every voter |
| `ELECTION_TIMEOUT_MIN` / `ELECTION_TIMEOUT_MAX` | Controller election timeout range; defaults `900ms` / `1500ms` |
| `HEARTBEAT_INTERVAL` | Controller heartbeat interval; default `200ms` and must be less than half the minimum election timeout |
| `TLS_CERT_FILE`, `TLS_KEY_FILE` | Optional server TLS certificate/key pair |
| `TLS_CLIENT_CA_FILE`, `REQUIRE_CLIENT_CERT` | Optional mutual TLS client verification |
| `TLS_CA_FILE`, `TLS_CLIENT_CERT_FILE`, `TLS_CLIENT_KEY_FILE`, `TLS_SERVER_NAME` | CLI/Go-client trust root, optional client certificate and expected broker DNS name |
| `PEER_TLS_CERT_FILE`, `PEER_TLS_KEY_FILE`, `PEER_TLS_CA_FILE` | Optional TLS certificate/key and trust CA for inter-broker traffic |
| `REQUIRE_PEER_CERT` | Require and verify a client certificate on the peer listener; requires peer TLS certificate/key and CA |
| `CLIENT_API_KEY`, `CLIENT_ACL` | Optional binary client authentication and topic/action ACL rules |
| `ADMIN_API_KEY` | Optional Bearer-key protection for admin HTTP routes |
| `ADMIN_ADDRESS` | Optional separate HTTP health/metrics listener; bind privately even when an API key is configured |
| `ADMIN_API_KEY` | Optional Bearer token protecting admin/diagnostic routes; health probes remain unauthenticated |
| `CLIENT_API_KEY` | Optional binary TCP AUTH token; clients must authenticate once per connection before normal requests |
| `CLIENT_ACL` | Optional semicolon-separated `role:topic:action` rules, e.g. `client:orders:produce;client:orders:consume` |

For TLS, configure `TLS_CERT_FILE` and `TLS_KEY_FILE`. For mTLS, also configure `TLS_CLIENT_CA_FILE` and set `REQUIRE_CLIENT_CERT=true`. The reusable Go client and CLI support TLS/mTLS and AUTH. The CLI reads `MESSLESSMQ_CLIENT_API_KEY`, `MESSLESSMQ_TLS_CA_FILE`, `MESSLESSMQ_TLS_CLIENT_CERT_FILE`, `MESSLESSMQ_TLS_CLIENT_KEY_FILE`, and `MESSLESSMQ_TLS_SERVER_NAME` from the environment. Configure the TLS CA and expected server name when connecting to a TLS-enabled listener.

## Runtime operations

With `MESSLESSMQ_ADMIN_ADDRESS=127.0.0.1:9093`, the runtime exposes:

- `GET /health/live`
- `GET /health/ready`
- `GET /diagnostics`
- `GET /metrics`
- `GET /controller/status` (only when `CONTROLLER_VOTERS` is configured)
- `GET /controller/metadata` (committed broker registrations and partition assignments)
- `POST /controller/reassignments/stage` with `{"topic":"orders","partition_id":0,"replicas":["broker-2","broker-3"]}`
- `POST /controller/reassignments/finalize` with `{"topic":"orders","partition_id":0}`

The management listener is separate from the binary client listener. Do not expose it to untrusted networks. When peer membership is enabled, it also exposes `GET /cluster/members`, which returns the local broker's current membership snapshot.

## Broker membership discovery (optional)

To enable membership discovery, configure a shared `MESSLESSMQ_CLUSTER_ID`, a unique `MESSLESSMQ_BROKER_ID`, and both peer addresses on every broker. A seed string has the form `broker-id@peer-host:port`. The first broker can start without seeds or can list itself as its only seed; self-seeds are ignored after validating that the address matches its advertised peer address. Later brokers contact a seed, learn its snapshot, and contact discovered peers so the membership view propagates through the reachable brokers.

Example bootstrap broker:

```bash
MESSLESSMQ_BROKER_ID=broker-1 \
MESSLESSMQ_CLUSTER_ID=orders-cluster \
MESSLESSMQ_LISTEN_ADDRESS=127.0.0.1:9092 \
MESSLESSMQ_ADVERTISE_ADDRESS=127.0.0.1:9092 \
MESSLESSMQ_PEER_LISTEN_ADDRESS=127.0.0.1:19093 \
MESSLESSMQ_PEER_ADVERTISE_ADDRESS=127.0.0.1:19093 \
MESSLESSMQ_DATA_DIR=./data/broker-1 \
go run ./cmd/broker
```

Example joining broker:

```bash
MESSLESSMQ_BROKER_ID=broker-2 \
MESSLESSMQ_CLUSTER_ID=orders-cluster \
MESSLESSMQ_LISTEN_ADDRESS=127.0.0.1:9094 \
MESSLESSMQ_ADVERTISE_ADDRESS=127.0.0.1:9094 \
MESSLESSMQ_PEER_LISTEN_ADDRESS=127.0.0.1:19094 \
MESSLESSMQ_PEER_ADVERTISE_ADDRESS=127.0.0.1:19094 \
MESSLESSMQ_SEEDS=broker-1@127.0.0.1:19093 \
MESSLESSMQ_DATA_DIR=./data/broker-2 \
go run ./cmd/broker
```

If `MESSLESSMQ_ADMIN_ADDRESS=127.0.0.1:9093` is configured, inspect a broker's current membership with `curl http://127.0.0.1:9093/cluster/members`.

For brokers with peer membership enabled, the broker persists `cluster_id` and `broker_id` in `<DATA_DIR>/metadata/identity.json`. Reusing that data directory with a different cluster or broker ID is rejected. Give every broker its own data directory/PVC. Peer TLS/mTLS can be enabled with `PEER_TLS_CERT_FILE`, `PEER_TLS_KEY_FILE`, `PEER_TLS_CA_FILE`, and `REQUIRE_PEER_CERT=true`. Membership still has no heartbeat-based removal protocol; membership snapshots are discovery information, not a consensus-committed controller view. Peer certificate identity is not yet bound to the claimed broker ID, so keep peer traffic on a trusted network even when TLS is enabled.
See [CLUSTER-MEMBERSHIP-GUIDE.md](CLUSTER-MEMBERSHIP-GUIDE.md) for a three-terminal local setup. `scripts/membership-smoke.sh` builds and launches three separate broker OS processes with isolated temporary data directories, then verifies that each admin membership endpoint reports all three brokers.

## Controller-committed cluster metadata and replication

To enable the distributed control plane, configure the same fixed voter list on every broker, including the local broker ID. Configure a shared cluster ID, unique broker IDs, peer listeners/advertised addresses, and seed peers as described above:

```bash
MESSLESSMQ_CONTROLLER_VOTERS=broker-1,broker-2,broker-3
MESSLESSMQ_MIN_ISR=2
```

If `MESSLESSMQ_REPLICA_ASSIGNMENTS` is supplied, every broker must use the same ordered map. The first replica is the initial partition leader. If no explicit map is supplied, the controller bootstrap derives a deterministic full-replica assignment from the sorted fixed voter IDs and rotates the initial leader by partition ID. This is a bootstrap policy, not a rack/load-aware placement algorithm.

After all configured voters are visible through membership, the elected controller leader proposes broker registrations and partition assignments through its Raft log. Assignments become authoritative only after a majority commits them. Every broker applies the committed snapshot to its metadata API and replication routing registry. `GET /controller/metadata` exposes the committed controller snapshot; the normal METADATA API exposes the broker/partition view used by clients. `GET /controller/status` reports local election state. Its `ready` field means voter IDs are visible in local membership; it does not guarantee that all controller metadata has been committed/applied.

Controller term, vote, log, and commit index persist under `<DATA_DIR>/metadata/controller/raft-state.json`. A restarted voter comes back as a follower. Data replication uses independent per-broker commit stores under `<DATA_DIR>/metadata/commits/`; consumers hide offsets at or beyond their local exclusive high watermark.

The controller commits broker registration, partition assignment, conservative ISR metadata, and staged reassignment intent for a fixed voter set. Existing committed assignments are authoritative and startup reconciliation will not overwrite them. When the current partition leader becomes unreachable, the elected controller probes it repeatedly, verifies the other assigned replicas' durable commit state, and commits a new leader with an incremented epoch only if a replica's log ends exactly at the highest observed high watermark. The old leader's controller-authority lease fences it from further writes after lease expiry. A follower is admitted to ISR only after its durable high watermark and physical log end match the leader; it is removed only after three consecutive incompatible or failed probes. Background catch-up copies to replicas outside ISR. Staged reassignment commits a union replica set, waits for target ISR admission, and only then finalizes the target set. The Go client supports keyed/unkeyed produce and consumer fetch routing. Peer TLS/mTLS is available but peer certificate identity binding and membership failure/removal remain incomplete.

`scripts/controller-metadata-smoke.sh` starts three independent broker processes, verifies committed broker and partition metadata, checks the normal METADATA API, and produces/consumes a record from both follower logs. `scripts/replication-smoke.sh` verifies durable record replication, commit markers, committed-read filtering, and MinISR behavior. `scripts/controller-election-smoke.sh` verifies controller-leader replacement after process failure and old-voter rejoin. `scripts/partition-leader-failover-smoke.sh` kills a partition leader, verifies safe controller-committed promotion, routes keyed production to the new leader, restarts the old broker, and verifies replica catch-up. See [CONTROLLER-ELECTION-GUIDE.md](CONTROLLER-ELECTION-GUIDE.md), [CONTROLLER-METADATA-GUIDE.md](CONTROLLER-METADATA-GUIDE.md), and [PARTITION-LEADER-FAILOVER-GUIDE.md](PARTITION-LEADER-FAILOVER-GUIDE.md).

## Tests and validation

```bash
go test -count=1 ./...
```

```bash
go test -race -count=1 ./...
```

```bash
go build ./cmd/broker ./cmd/client
```

```bash
go vet ./...
```

```bash
./scripts/integration.sh
```

```bash
./scripts/data-integrity.sh
```

```bash
./scripts/membership-smoke.sh
```

```bash
./scripts/replication-smoke.sh
```

```bash
./scripts/controller-election-smoke.sh
```

```bash
./scripts/controller-metadata-smoke.sh
```

```bash
./scripts/partition-leader-failover-smoke.sh
```

```bash
./scripts/reassignment-smoke.sh
```

Run all package and multi-process smoke checks with:

```bash
./scripts/smoke-all.sh
```

`scripts/crash-restart.sh` requires `nc` and port `19092` to be free. It produces a synchronously acknowledged record, sends SIGKILL to the broker, restarts it on the same temporary data directory, and verifies the record is still readable. This tests local log durability/recovery, not replication.

## Docker Compose

```bash
docker compose -f deploy/docker-compose.yml up --build -d
```

The three services listen on host ports `19092`, `19093`, and `19094`. Each has a separate named data volume, seed-based membership discovery, and the same fixed three-voter controller set. With no explicit replica map, the controller commits a deterministic full-replica assignment for each bootstrap topic partition; the manifests set `MESSLESSMQ_MIN_ISR=2`. Their admin endpoints are published only on loopback host ports `19101`, `19102`, and `19103`. Inspect membership with `/cluster/members`, controller state with `/controller/status`, and committed assignments with `/controller/metadata`.

## Secure local development

Generate local development credentials with `./scripts/generate-dev-certs.sh`, then follow [docs/SECURITY.md](docs/SECURITY.md) to start the secure Docker Compose overlay or apply the secure Kubernetes Kustomize overlay. The generated certificates are not production PKI material.

## Kubernetes

For a local Kind cluster named `messlessmq`:

```bash
docker build -t messlessmq:integrated .
```

```bash
kind load docker-image messlessmq:integrated --name messlessmq
```

```bash
kubectl apply -k deploy/kubernetes/
```

Check storage support before applying the StatefulSet:

```bash
kubectl get storageclass
```

The StatefulSet requests one 5Gi persistent volume claim per pod; the cluster must have a default StorageClass or a configured storage provisioner. If the existing StatefulSet was created from the earlier no-PVC manifest, its `volumeClaimTemplates` cannot be added by an in-place update. Back up any data you need, then recreate the StatefulSet before applying the new manifest. The TCP readiness probe only checks that the listener accepts connections.

## Security and feature boundaries

- Client TLS/mTLS, peer TLS/mTLS, binary API-key AUTH, topic ACLs, and optional admin Bearer-key protection are integrated into the live request paths.
- The default Docker Compose/Kubernetes manifests are development/lab configurations; they do not provision certificates or secrets. Peer/admin listeners must remain on trusted networks until deployment security is configured.
- Peer TLS verifies certificate trust and advertised hostnames, but does not yet bind certificate identity to broker IDs.
- Consumer GROUP v1 operations are exposed over the wire and offsets are durable. Group membership/assignments remain local to one broker process and in-memory; coordinator failover requires members to rejoin.
- Controller proposals use a fixed voter set. Per-follower `nextIndex`/`matchIndex` and conflict-hint backtracking are implemented, but snapshots, snapshot install, log compaction, and voter reconfiguration are not.
- Dynamic ISR and staged replica reassignment are implemented with catch-up-before-admission/finalization. Automatic divergent-replica truncation/rebootstrap is not implemented.
- The reusable Go client and CLI support keyed/unkeyed produce routing, consumer fetch routing, metadata caching, TLS and API-key AUTH. Uncertain producer outcomes are not automatically retried.

Do not describe the current build as a production-ready Kafka replacement. It is a functioning persistent multi-broker system with synchronous replication, committed-read boundaries, fixed-voter controller metadata, partition failover, ISR tracking, staged reassignment, client routing, and a versioned consumer-group API. Remaining production work includes Raft snapshot/recovery, safe voter changes, distributed group coordinator recovery, divergent replica rebootstrap, peer identity authorization, credential rotation, and broader chaos/soak/load testing.
