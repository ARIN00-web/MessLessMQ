# MessLessMQ

MessLessMQ is a from-scratch Go message broker project. Each broker process provides a persistent TCP data path with durable topics/partitions, PRODUCE, CONSUME, and METADATA APIs, a small CLI, optional TLS/mTLS, metrics, health endpoints, request limits, and graceful shutdown. An optional peer listener supports seed-based broker membership discovery and a fixed-voter, durable Raft controller election across independent processes.

The repository contains implementations of replication, controller consensus, cluster metadata, inter-broker transport, consumer groups, offset management, and recovery. Broker membership, synchronous remote record replication, durable commit markers, and fixed-voter controller elections are now wired across independent processes. Partition assignments are still statically configured; controller metadata is not yet authoritative for assignment changes, and controller elections do not yet promote partition leaders in the data plane.

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

The producer routes keyed messages deterministically to a partition. With one partition, every message routes to partition zero. For multiple partitions, inspect the metadata/produce response to determine the selected partition before consuming.

## API versions

| API | Version | Behavior |
|---|---:|---|
| PRODUCE | 1 | Append messages and return topic, partition, and offset results |
| CONSUME | 1 | Legacy value-only response |
| CONSUME | 2 | Key/value response with timestamp and offsets |
| METADATA | 1 | Known broker membership and statically configured partition replica/leader metadata |

`acks=leader` synchronizes the local log before acknowledging. When static replica assignments are configured, `acks=all` waits for the configured MinISR data writes and durable commit-marker acknowledgements; without replica assignments it is local-only. A produce batch is not transactional if a storage error occurs after earlier records in the batch were appended.

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
| `MIN_ISR` | Minimum number of replicas required for `acks=all`; defaults to `1` |
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
| `ADMIN_ADDRESS` | Optional separate HTTP health/metrics listener; bind privately because it has no authentication middleware |

For TLS, configure `TLS_CERT_FILE` and `TLS_KEY_FILE`. For mTLS, also configure `TLS_CLIENT_CA_FILE` and set `REQUIRE_CLIENT_CERT=true`. The included CLI currently supports plaintext TCP only; use a TLS-capable client for a TLS-enabled listener.

## Runtime operations

With `MESSLESSMQ_ADMIN_ADDRESS=127.0.0.1:9093`, the runtime exposes:

- `GET /health/live`
- `GET /health/ready`
- `GET /diagnostics`
- `GET /metrics`
- `GET /controller/status` (only when `CONTROLLER_VOTERS` is configured)

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

For brokers with peer membership enabled, the broker persists `cluster_id` and `broker_id` in `<DATA_DIR>/metadata/identity.json`. Reusing that data directory with a different cluster or broker ID is rejected. Give every broker its own data directory/PVC. Peer join currently uses plaintext, unauthenticated TCP and has no heartbeat-based membership removal. Keep peer listeners on a trusted private network. Membership snapshots are per-process and in-memory; they are discovery information, not a consensus-committed controller view. The membership subsystem itself does not replicate records or elect partition leaders; those are separate runtime features described below.
See [CLUSTER-MEMBERSHIP-GUIDE.md](CLUSTER-MEMBERSHIP-GUIDE.md) for a three-terminal local setup. `scripts/membership-smoke.sh` builds and launches three separate broker OS processes with isolated temporary data directories, then verifies that each admin membership endpoint reports all three brokers.

## Static replication and controller elections

To replicate a partition, configure the same static ordered replica assignment on every broker. The first broker is the fixed data leader. For example:

```bash
MESSLESSMQ_REPLICA_ASSIGNMENTS='orders:0=broker-1,broker-2,broker-3;orders:1=broker-2,broker-3,broker-1;orders:2=broker-3,broker-1,broker-2'
MESSLESSMQ_MIN_ISR=2
```

To enable the separate fixed-voter controller, also configure the same voter list on every broker, including its own broker ID:

```bash
MESSLESSMQ_CONTROLLER_VOTERS=broker-1,broker-2,broker-3
```

The controller persists term, vote, log, and commit index under `<DATA_DIR>/metadata/controller/raft-state.json`; controller roles are volatile and a restarted voter rejoins as a follower. `GET /controller/status` reports election state. The `ready` field means all configured voter IDs are visible in the local membership snapshot; it is not a general health or quorum-commit guarantee. The controller currently elects a controller leader and exchanges Raft heartbeats, but it does not yet commit runtime broker/partition assignment proposals or trigger data-partition leader changes. Data-plane replica leaders remain the first broker in the static assignment.

`scripts/replication-smoke.sh` verifies three-process record replication, durable commit markers, committed-read filtering, and MinISR behavior. `scripts/controller-election-smoke.sh` verifies a real three-process controller election, controller-leader process failure, replacement election, and restart of the old voter as a follower. See [CONTROLLER-ELECTION-GUIDE.md](CONTROLLER-ELECTION-GUIDE.md) for voter configuration, state persistence, and status inspection.

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

`scripts/crash-restart.sh` requires `nc` and port `19092` to be free. It produces a synchronously acknowledged record, sends SIGKILL to the broker, restarts it on the same temporary data directory, and verifies the record is still readable. This tests local log durability/recovery, not replication.

## Docker Compose

```bash
docker compose -f deploy/docker-compose.yml up --build -d
```

The three services listen on host ports `19092`, `19093`, and `19094`. Each has a separate named data volume, seed-based membership discovery, and the same fixed three-voter controller set. Their admin endpoints are published only on loopback host ports `19101`, `19102`, and `19103`. Controller elections are active, but message replication still requires matching `MESSLESSMQ_REPLICA_ASSIGNMENTS` and `MESSLESSMQ_MIN_ISR` configuration on all three services. Inspect membership with `/cluster/members` and controller state with `/controller/status`.

## Kubernetes

For a local Kind cluster named `messlessmq`:

```bash
docker build -t messlessmq:integrated .
```

```bash
kind load docker-image messlessmq:integrated --name messlessmq
```

```bash
kubectl apply -f deploy/kubernetes/
```

Check storage support before applying the StatefulSet:

```bash
kubectl get storageclass
```

The StatefulSet requests one 5Gi persistent volume claim per pod; the cluster must have a default StorageClass or a configured storage provisioner. If the existing StatefulSet was created from the earlier no-PVC manifest, its `volumeClaimTemplates` cannot be added by an in-place update. Back up any data you need, then recreate the StatefulSet before applying the new manifest. The TCP readiness probe only checks that the listener accepts connections.

## Security and feature boundaries

- New runtime-produced messages wrap key/value in a CRC32-checked internal envelope.
- API-key authentication and ACL packages exist but are not wired into the live custom TCP request pipeline.
- Runtime rate limiting is keyed by remote IP rather than authenticated principal.
- Consumer group coordination and durable offset APIs exist as packages but are not exposed end-to-end through the public request protocol.
- Remote record replication and durable commit markers work with static replica assignments, but assignments are not yet sourced from committed controller metadata.
- A durable fixed-voter controller election runs across broker processes, but controller metadata proposals are not yet wired into the running broker.
- Partition leader promotion, dynamic ISR changes, replica reassignment, and automatic client rerouting are not yet implemented.
- Peer RPCs are plaintext and unauthenticated; admin endpoints are unauthenticated and must remain private. Consumer-group membership and offset commit/fetch are not yet exposed as a complete wire protocol.

Do not describe the current build as a production-ready replicated Kafka replacement. It is a functioning persistent broker with a statically configured multi-process replication path and fixed-voter controller elections; it still needs controller-driven partition failover, secure peer RPCs, and end-to-end client routing.
