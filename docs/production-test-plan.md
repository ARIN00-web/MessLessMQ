# MessLessMQ Production & Distributed Test Plan

This plan is intentionally split between tests the current repository can run now and experiments that require the control plane and data plane to be wired into the broker executable.

## Test layers

| Layer | Command / asset | What it establishes | What it does not establish |
|---|---|---|---|
| Unit | `go test ./...` | Package-level behavior and regressions | Real multi-process behavior |
| Race | `go test -race ./...` | Data races exercised by the test suite | Absence of every possible race under every workload |
| Integration | `scripts/integration.sh` | Existing request, protocol, cluster, and consumer package integration tests | A running multi-broker cluster |
| Data integrity | `scripts/data-integrity.sh` | Storage, replication, offsets, delivery, and recovery test suites | End-to-end durability across actual broker processes |
| Microbenchmark | `scripts/benchmark.sh` | Relative CPU/allocation costs of implemented primitives | End-to-end broker throughput or production latency |
| Process crash | `scripts/crash-restart.sh` | Listener exits after SIGKILL and can be restarted; SIGTERM path exits | Message recovery, because the current executable does not compose the storage/replication stack |
| Container topology | `deploy/docker-compose.yml` | Three isolated broker processes can be launched and reached on distinct host ports | Replication, leader election, partition routing, or shared metadata |

## Repeatable local validation

Run `scripts/validate.sh` for full tests, the race detector, and `go vet`.

Run `scripts/integration.sh` when investigating network/protocol/consumer behavior.

Run `scripts/data-integrity.sh` after changing storage, replication, offset, record, or recovery code.

Run `scripts/benchmark.sh` before and after a performance change. Record the Go version, OS, architecture, CPU, benchmark flags, and commit. Compare the same machine and same flags; do not compare numbers from unrelated machines as if they were equivalent.

Run `scripts/crash-restart.sh` only when port 9092 is free. The script builds the broker, starts it, kills the process, restarts it, then tests SIGTERM shutdown. It is a process lifecycle smoke test, not a data durability test.

## Distributed failure matrix

The following experiments are required before claiming production-grade distributed behavior. Several need runtime wiring that is still outstanding.

| Experiment | Fault injection | Required invariant | Current status |
|---|---|---|---|
| Broker process crash | Kill a broker process during produce | Acknowledged records remain available according to configured acknowledgement policy | Not end-to-end wired |
| Follower isolation | Block leader-to-follower traffic | ISR and high watermark do not advance using an unavailable follower | Manager-level tests exist; live TCP path pending |
| Leader crash | Kill partition leader | Only an eligible in-sync replica can be promoted; stale leader epoch is fenced | In-memory manager tests; live cluster orchestration pending |
| Controller majority loss | Isolate enough controllers to remove quorum | No minority controller commits metadata | Controller simulation tests; persistent/networked consensus pending |
| Replica lag | Pause follower, continue writes, resume follower | Follower catches up before rejoining ISR | Manager/recovery tests; live process integration pending |
| Disk full / write failure | Use a constrained filesystem or injected writer | Failed append is not acknowledged as durable | Unit/failure tests; filesystem chaos job pending |
| Corrupt record | Flip bytes or truncate segment | Corruption is detected or explicitly reported | Structural verification exists; record checksum is not implemented |
| Consumer crash | Kill consumer after side effect and before offset commit | Redelivery follows the selected delivery semantics | Package tests exist; external side-effect integration pending |
| Network partition | Drop packets between broker/controller processes | No split-brain leader or conflicting committed metadata | Simulated-link tests exist; live cluster test pending |

## CI guidance

A CI pipeline should run, at minimum:

1. `go test -count=1 ./...`
2. `go test -race -count=1 ./...`
3. `go vet ./...`
4. `go test -run '^$' -bench . -benchmem ./internal/performance` as a reported benchmark job, not a flaky hard threshold.
5. Docker image build and YAML validation.

Do not make benchmark nanoseconds a hard pass/fail gate until there is a stable runner and statistically defensible baseline. For distributed chaos jobs, preserve logs, broker IDs, controller terms, leader epochs, high watermarks, and test seeds as artifacts.

## Exit criteria for a production release

- The executable composes the protocol dispatcher, topic/partition storage, replication manager, controller/metadata, authentication, authorization, TLS, metrics, health checks, rate limiting, and graceful-drain controls.
- Broker identity, advertised address, data directory, and listener security are loaded from validated configuration.
- Controller term/vote/log/commit state and cluster metadata survive process restart.
- Replication is carried over the inter-broker transport, with explicit timeout, retry, authentication, and fencing behavior.
- Topic and partition metadata are durable and reconciled with on-disk logs.
- Record checksums and a documented corruption-recovery policy exist.
- A real three-broker test proves produce, replication, leader failure, election, catch-up, and consume across processes.
- Load, crash, network-partition, and disk-failure tests pass on repeatable infrastructure.
- SLOs are defined for availability, p95/p99 latency, throughput, recovery time, and data loss (RPO).
