# MessLessMQ — Production & Distributed Test Plan

This plan distinguishes the currently integrated single-broker runtime from the distributed components that still need multi-process wiring.

## Test layers

| Layer | Command / asset | What it establishes | What it does not establish |
|---|---|---|---|
| Unit | `go test ./...` | Package behavior and regressions | Real multi-process behavior by itself |
| Race | `go test -race ./...` | Data races exercised by the test suite | Absence of every possible race under every workload |
| Runtime integration | `go test ./internal/server` | Real TCP produce, consume, local metadata, log persistence and restart recovery in one broker process | Remote replication or controller consensus |
| Integration | `scripts/integration.sh` | Runtime, request, protocol, cluster, inter-broker, consumer, group, and offset package integration tests | A functioning multi-broker cluster |
| Data integrity | `scripts/data-integrity.sh` | Storage, replication, offsets, delivery, record metadata, and recovery suites | End-to-end replica durability across actual broker processes |
| Microbenchmark | `scripts/benchmark.sh` | Relative CPU/allocation costs of performance primitives | End-to-end broker throughput or production latency |
| Single-broker crash recovery | `scripts/crash-restart.sh` | A synchronously acknowledged keyed message remains readable after SIGKILL and restart using the same data directory; SIGTERM path exits | Replica recovery or failover |
| Docker Compose | `deploy/docker-compose.yml` | Three standalone broker processes with separate named persistent volumes | Replication, election, shared metadata, or routing |
| Kubernetes | `deploy/kubernetes/` | Three StatefulSet pods with stable identities and per-pod PVCs, subject to a working StorageClass | Replication, election, shared metadata, or routing |

## Repeatable local validation

Run `scripts/validate.sh` for the full tests, race detector, command builds, and `go vet`.

Run `scripts/integration.sh` for the runtime TCP test and the adjacent network/protocol/consumer packages.

Run `scripts/data-integrity.sh` after changing storage, replication, offset, record, or recovery code.

Run `scripts/crash-restart.sh` only when port `19092` is free and `nc` is installed. It builds the broker and CLI, produces one record with `acks=leader`, kills the broker with SIGKILL, restarts it with the same temporary data directory, consumes from offset zero, verifies the value, and then tests SIGTERM shutdown. This proves local single-broker log recovery for that experiment; it does not prove replica recovery.

Run `scripts/benchmark.sh` before and after a performance change. Record Go version, OS, architecture, CPU, flags, and commit. Compare the same machine and flags.

## Distributed failure matrix

| Experiment | Required invariant | Current status |
|---|---|---|
| Single broker process crash | Sync-acknowledged local records recover from the same data directory | Automated script and runtime test included |
| Pod replacement | StatefulSet recreates pod identity and reattaches its PVC | Manifest supports it; requires a provisioned PVC |
| Follower isolation | ISR/high watermark do not advance using an unavailable follower | Replication manager tests exist; live TCP path pending |
| Leader crash | Only an eligible in-sync replica can be promoted; stale leader epoch is fenced | In-memory manager tests; live cluster orchestration pending |
| Controller majority loss | No minority controller commits metadata | Controller simulation tests; persistent/networked consensus pending |
| Replica lag | Follower catches up before rejoining ISR | Manager/recovery tests; live process integration pending |
| Disk full/write failure | Failed append is not acknowledged as durable | Unit/failure tests; filesystem chaos job pending |
| Record corruption | Checksummed runtime message envelope detects payload corruption | CRC32 envelope tests; no universal segment checksum for arbitrary raw storage records |
| Consumer crash | Redelivery follows selected delivery semantics | Package tests; external side-effect integration pending |
| Network partition | No split-brain leader or conflicting committed metadata | Simulated-link tests; live cluster test pending |

## CI guidance

A CI pipeline should run:

1. `go test -count=1 ./...`
2. `go test -race -count=1 ./...`
3. `go build ./cmd/broker ./cmd/client`
4. `go vet ./...`
5. YAML validation and Docker image build.
6. The single-broker crash/restart script on a runner with `nc`.

Do not make benchmark nanoseconds a hard pass/fail gate until there is a stable runner and statistically defensible baseline. For distributed chaos jobs, preserve logs, broker IDs, terms, leader epochs, high watermarks, test seeds, and volume identities as artifacts.

## Exit criteria for a distributed production release

- Persist controller term/vote/log/commit state and metadata.
- Run a real multi-process controller quorum over authenticated/TLS peer transport.
- Connect replication batches to remote brokers and enforce ISR/high-watermark semantics across processes.
- Route clients to leaders using current cluster metadata.
- Expose topic administration, group membership, and offset APIs over the versioned protocol.
- Integrate API-key auth/ACLs and secure client TLS.
- Validate data format migration and corruption handling.
- Pass end-to-end three-broker produce, failover, catch-up, and consume tests.
- Define SLOs for availability, p95/p99 latency, throughput, recovery time, and RPO.
