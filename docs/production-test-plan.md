# MessLessMQ — Production & Distributed Test Plan

This plan distinguishes package tests, the persistent broker runtime, multi-process data replication, fixed-voter controller elections, and controller-driven partition leader failover.

## Test layers

| Layer | Command / asset | What it establishes | What it does not establish |
|---|---|---|---|
| Unit | `go test ./...` | Package behavior and regressions | Real multi-process behavior by itself |
| Race | `go test -race ./...` | Data races exercised by the suite | Absence of every possible race under every workload |
| Runtime integration | `go test ./internal/server` | Real TCP produce, consume, metadata, local log persistence and restart recovery | Remote replication or controller elections |
| Integration | `scripts/integration.sh` | Runtime, request, protocol, cluster, peer, consumer, group, and offset package tests | Full autonomous cluster behavior |
| Membership | `scripts/membership-smoke.sh` | Three broker processes converge on a shared discovered membership view | Liveness-based membership removal or consensus membership |
| Replication/commit | `scripts/replication-smoke.sh` | Real record copies to followers, durable commit markers, committed-read filtering, and MinISR behavior | Dynamic leader election or automatic replica reassignment |
| Controller election | `scripts/controller-election-smoke.sh` | Three broker processes elect one controller leader, elect a replacement after its process stops, and restart the old voter from durable state as a follower | Dynamic voter reconfiguration and full partition failover |
| Controller metadata | `scripts/controller-metadata-smoke.sh` | Three broker registrations and partition assignments are committed by the controller, applied on all brokers, exposed by METADATA, and used by replication | Dynamic placement/reassignment |
| Partition leader failover | `scripts/partition-leader-failover-smoke.sh` | Detects a failed partition leader, promotes only an assigned replica with a complete committed prefix, refreshes keyed client metadata, continues writes/reads, restarts the old leader, and verifies catch-up | Divergent replica truncation/rebootstrap, dynamic ISR, unkeyed/client-SDK routing |
| Data integrity | `scripts/data-integrity.sh` | Storage, replication, offsets, delivery, record metadata, and recovery suites | All possible multi-process data corruption scenarios |
| Microbenchmark | `scripts/benchmark.sh` | Relative CPU/allocation cost of performance primitives | End-to-end broker throughput or production latency |
| Single-broker crash recovery | `scripts/crash-restart.sh` | Synchronously acknowledged local record remains readable after SIGKILL/restart on same data directory | Replica recovery or leader failover |
| Docker Compose | `deploy/docker-compose.yml` | Three broker processes, membership, fixed-voter controller elections, controller-committed assignments, synchronous replication, and separate named volumes | Dynamic ISR/reassignment and secure peer transport |
| Kubernetes | `deploy/kubernetes/` | Three StatefulSet pods with stable identities and per-pod PVCs, subject to a working StorageClass; fixed-voter elections, committed assignments, replication, and in-set partition failover when deployed with the expected config | Dynamic ISR/reassignment, secure peer transport, and end-to-end consumer routing |

## Repeatable local validation

Run `scripts/validate.sh` for full tests, race detection, command builds, and `go vet`.

Run `scripts/membership-smoke.sh` to exercise broker discovery across three processes. Run `scripts/replication-smoke.sh` to exercise synchronous static-assignment replication, durable commit markers, committed-read filtering, and MinISR behavior. Run `scripts/controller-election-smoke.sh` to exercise a durable fixed-voter controller election, stop the elected controller process, verify a replacement election, and restart the old voter from persisted term/vote/log/commit state. Run `scripts/controller-metadata-smoke.sh` to verify committed registrations and partition assignments on all brokers and the METADATA/data path. Run `scripts/partition-leader-failover-smoke.sh` to stop a data-partition leader, verify controller-committed promotion and keyed client rerouting, then restart the old leader and verify it catches up.

The controller election smoke script uses client ports `29392`–`29394`, peer ports `29492`–`29494`, and admin ports `29592`–`29594`; these ports must be free. It requires `curl`, `nc`, and `python3`. The replication script has its own documented port range in `DISTRIBUTED-REPLICATION-GUIDE.md`.

Run `scripts/crash-restart.sh` only when port `19092` is free and `nc` is installed. It builds the broker and CLI, produces one record with `acks=leader`, kills the broker with SIGKILL, restarts it with the same temporary data directory, consumes from offset zero, verifies the value, and then tests SIGTERM shutdown. This proves local log recovery, not replica failover.

Run `scripts/benchmark.sh` before and after a performance change. Record Go version, OS, architecture, CPU, flags, and commit. Compare the same machine and flags.

## Distributed failure matrix

| Experiment | Required invariant | Current status |
|---|---|---|
| Single broker process crash | Sync-acknowledged local records recover from same data directory | Automated script and runtime test included |
| Broker process membership | Seed join discovers all configured brokers | Three-process membership smoke test passes; no failure detector/removal |
| Follower process unavailable | Writes proceed only if MinISR still holds | Three-process replication smoke test covers one follower down and both followers down |
| Uncommitted log tail | Consumers do not read offsets at/above high watermark | Replication smoke test covers leader and follower filtering |
| Controller leader process failure | A majority elects one replacement; restarted old voter rejoins as follower | Three-process durable controller-election smoke test included |
| Controller quorum loss | No minority controller wins an election or commits metadata | Majority vote and proposal quorum enforced; a real network-partition chaos test remains pending |
| Partition leader process failure | Only an assigned replica with a complete committed prefix may be promoted under a higher leader epoch | Three-process failover smoke test included; failover is refused if no candidate proves a clean committed prefix |
| Old partition leader returns | Old leader must be fenced, apply the new epoch, and catch up before serving writes | Three-process failover smoke test verifies new epoch application and catch-up on a later append; explicit dynamic ISR rejoin state is still pending |
| Disk full/write failure | Failed append is not acknowledged as durable | Unit/failure tests; filesystem chaos job pending |
| Record corruption | Checksummed runtime message envelope detects payload corruption | CRC32 envelope tests; no universal segment checksum for arbitrary raw storage records |
| Consumer crash | Redelivery follows selected delivery semantics | Package tests; external side-effect integration pending |
| Network partition | No split-brain controller leader | Fixed-voter majority election tested via process kill; live network-partition chaos and full Raft metadata proposal tests pending |

## CI guidance

A CI pipeline should run:

1. `go test -count=1 ./...`
2. `go test -race -count=1 ./...`
3. `go build ./cmd/broker ./cmd/client`
4. `go vet ./...`
5. YAML validation and Docker image build.
6. Membership, replication, controller-election, committed-metadata, partition-leader-failover, and single-broker crash/restart smoke tests on an isolated runner.

Do not make benchmark nanoseconds a hard pass/fail gate until there is a stable runner and statistically defensible baseline. For distributed chaos jobs, preserve logs, broker IDs, terms, leader epochs, high watermarks, test seeds, and volume identities as artifacts.

## Exit criteria for a distributed production release

- Commit broker registration and partition assignments through the live controller Raft log and apply committed metadata on every broker.
- Make committed controller metadata authoritative for replica sets and partition leader epochs.
- Extend safe partition promotion with dynamic ISR membership, staged replica reassignment, and a snapshot/truncation/rebootstrap path for replicas with divergent uncommitted suffixes.
- Extend client routing to unkeyed produce and consumer fetch, and provide a reusable client metadata cache with bounded retries.
- Add authenticated/TLS-protected peer communication and safe voter reconfiguration.
- Expose topic administration, group membership, and offset APIs over the versioned protocol.
- Integrate API-key auth/ACLs and secure client TLS.
- Validate data format migration and corruption handling.
- Pass end-to-end three-broker produce, partition failover, catch-up, and consume tests.
- Define SLOs for availability, p95/p99 latency, throughput, recovery time, and RPO.
