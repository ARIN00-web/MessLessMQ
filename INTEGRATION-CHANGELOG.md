# MessLessMQ — Runtime Integration Changelog

This delivery integrates the existing package work into a persistent single-broker executable. It does not yet wire remote replication or controller consensus across independent broker processes.

## Runtime changes

- Added `internal/server` as the broker lifecycle composition root.
- Replaced the placeholder PRODUCE handler with a working API v1 implementation.
- Connected PRODUCE and CONSUME to one shared persistent topic registry.
- Added an atomic durable topic catalog at `DATA_DIR/metadata/topics.json` and startup recovery of topic partition logs.
- Added `MLMQMSG1` key/value envelopes with CRC32; CONSUME v2 returns persisted message keys while CONSUME v1 remains the value-only response format.
- Added METADATA API v1 for local standalone broker metadata.
- Added a small CLI with `produce`, `consume`, and `metadata` commands.
- Wired runtime request deadlines, connection limits, IP-based rate limiting, request metrics, optional TLS/mTLS, optional HTTP health/diagnostics/metrics, graceful drain, and topic-log close into the server lifecycle.
- Hardened network framing against empty/oversized frames and short writes.

## Deployment and operational changes

- Docker image configures a writable non-root data directory at `/var/lib/messlessmq`.
- Docker Compose uses separate named volumes and unique broker IDs for the three standalone process examples.
- Kubernetes StatefulSet uses stable pod identities and a per-pod 5Gi persistent volume claim.
- `scripts/integration.sh` includes the runtime integration package tests.
- `scripts/validate.sh` now builds broker and client binaries as well as running tests, race detection, and vet.
- `scripts/crash-restart.sh` now produces a synchronously acknowledged message, kills the broker, restarts against the same data directory, and verifies the message is still readable.
- Added runtime operations guide, ADR 0006, architecture review, and production test plan updates.

## Validation performed in the build environment

The uploaded `go.mod` declares Go 1.27.1. The build environment for this integration run has Go 1.23.2 and no network access to fetch the declared toolchain. For validation only, the `go` directive was temporarily changed to 1.23.2 and restored before packaging. Under Go 1.23.2:

- `go test -count=1 ./...` passed.
- `go test -race -count=1 ./...` passed.
- `go vet ./...` passed.
- `go build ./cmd/broker ./cmd/client` passed.
- `scripts/integration.sh` passed.
- `scripts/data-integrity.sh` passed.
- `scripts/crash-restart.sh` passed and verified a local sync-acknowledged record survived SIGKILL/restart.
- Bash syntax checks and YAML parsing passed.

Docker image build and Kubernetes deployment were not run in this build environment because Docker/kubectl are unavailable here. The `go.mod` file in this delivery remains set to Go 1.27.1; run the same validation on the target Go 1.27.1 toolchain.

## Explicit remaining limitations

- Replication is not connected to remote broker processes.
- Controller Raft state is not durably persisted or run as a networked quorum.
- Each Docker/Kubernetes broker is an independent standalone instance with its own data volume.
- The local metadata handler advertises only local partitions and does not consult the cluster controller.
- Client leader routing is not wired into the CLI or request path.
- Consumer groups and durable group offsets are not exposed through the public network protocol.
- API-key authentication and ACL authorization are not wired into the TCP request pipeline. TLS/mTLS is available on the live server listener when configured; the included CLI is plaintext-only.
- Admin HTTP endpoints are optional and unauthenticated; bind them only to a trusted private interface.
- Produce batches are not transactional. `acks=all` is local-only in this single-broker runtime.
- Record CRC covers new runtime-produced key/value envelopes, not arbitrary low-level/raw storage payloads.
