# MessLessMQ

MessLessMQ is a from-scratch Go message broker project. The current executable composes a **persistent single-broker runtime** with a TCP binary protocol, durable topics/partitions, PRODUCE, CONSUME, and METADATA APIs, a small CLI, optional TLS/mTLS, metrics, health endpoints, request limits, and graceful shutdown.

The repository also contains educational implementations of replication, controller consensus, cluster metadata, inter-broker transport, consumer groups, offset management, and recovery. Those distributed components are not yet wired together into an autonomous multi-process cluster. The three-container and three-pod deployment examples run independent brokers with separate data volumes; they do not replicate messages or elect a shared partition leader.

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
| METADATA | 1 | Local standalone broker and partition metadata |

`acks=leader` synchronizes the local log before acknowledging. In this standalone runtime `acks=all` is also local-only; it does not wait for a distributed ISR quorum. A produce batch is not transactional if a storage error occurs after earlier records in the batch were appended.

## Configuration

The broker reads environment variables prefixed with `MESSLESSMQ_`:

| Variable | Default / purpose |
|---|---|
| `BROKER_ID` | `broker-1` |
| `LISTEN_ADDRESS` | `127.0.0.1:9092` |
| `ADVERTISE_ADDRESS` | `127.0.0.1:9092`; follows an overridden listen address if not explicitly set |
| `DATA_DIR` | `./data` |
| `BOOTSTRAP_TOPICS` | Empty; comma-separated `name:partition-count` entries |
| `LOG_LEVEL` | `info` |
| `MAX_CONNECTIONS` | `10000` |
| `REQUEST_TIMEOUT` | `10s` |
| `SHUTDOWN_TIMEOUT` | `30s` |
| `RATE_LIMIT_PER_SECOND` | `1000` per remote IP |
| `RATE_LIMIT_BURST` | `2000` per remote IP |
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

The management listener is separate from the binary client listener. Do not expose it to untrusted networks.

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

`scripts/crash-restart.sh` requires `nc` and port `19092` to be free. It produces a synchronously acknowledged record, sends SIGKILL to the broker, restarts it on the same temporary data directory, and verifies the record is still readable. This tests local log durability/recovery, not replication.

## Docker Compose

```bash
docker compose -f deploy/docker-compose.yml up --build -d
```

The three services listen on host ports `19092`, `19093`, and `19094`. Each has a separate named data volume. These are independent standalone brokers, not a replicated cluster.

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
- Controller consensus, remote replication, dynamic metadata routing, and multi-broker failover remain future integration work.

Do not describe the current build as a production-ready replicated Kafka replacement. It is a functioning persistent standalone broker plus tested distributed-systems components and deployment labs.
