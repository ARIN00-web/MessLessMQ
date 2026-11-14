# MessLessMQ — Partition Leader Failover v1

This milestone adds controller-driven promotion of a replacement leader inside an already committed static replica set, plus keyed CLI producer routing from committed metadata.

## What happens when a partition leader fails

1. Only the currently elected controller leader runs the partition failure reconciler.
2. It probes each partition's committed leader. A failure is counted only when the status RPC fails; three consecutive failures are required before promotion is attempted. Each probe has a 350 ms timeout.
3. The controller queries the other brokers in the committed replica list.
4. A candidate must report the old assignment's commit leader ID and leader epoch. It must have the highest high watermark observed among valid replicas and its physical log end (`NextOffset`) must equal that high watermark. Its retained start offset must not be beyond the high watermark.
5. If there is no eligible candidate, the controller refuses to promote a replica. In particular, a replica with an uncommitted suffix is not eligible.
6. The controller proposes a new partition assignment through the fixed-voter Raft log, retaining the same replica list and incrementing `LeaderEpoch`.
7. Brokers apply the committed assignment and update their per-partition commit-store leader ID/epoch while preserving the high watermark.
8. The new leader accepts writes only while its controller-authority lease is valid. A leader renews its lease after a majority heartbeat; a follower renews after accepting AppendEntries from its current controller leader. A stale/isolated broker fails closed when its lease expires.

## Client routing

The CLI `produce` command automatically fetches METADATA for keyed messages, sorts the topic's partitions by ID, mirrors the broker's FNV-1a partitioner, finds the committed leader's advertised client address, and sends the request there. If it receives `NotPartitionLeader` or `ControllerAuthorityUnavailable`, it refreshes metadata once from the bootstrap address and retries once.

Automatic routing currently applies only to keyed messages. Unkeyed produce uses broker-local round-robin partition selection, so this simple CLI cannot calculate the selected partition before sending. The CLI consume command still takes an explicit broker address, topic, partition, and offset; consumer-fetch routing is not automatic.

## Run the three-process failover test

Ensure the following local TCP ports are free:

- Client: `29992`–`29994`
- Peer: `30092`–`30094`
- Admin: `30192`–`30194`

Then run:

```bash
chmod +x scripts/partition-leader-failover-smoke.sh
./scripts/partition-leader-failover-smoke.sh
```

The script builds the broker and client, launches three independent broker processes with separate temporary data directories, verifies committed membership/partition metadata, produces a committed record and reads it from both followers, stops the current partition leader, waits for controller-committed promotion, produces through metadata-based keyed routing, restarts the old broker from its original data/controller state, and verifies that the old replica receives the later committed records.

The smoke test is destructive only to its own temporary directories and terminates the processes it started. It does not use your normal `./data` directory.

## Safety limits and remaining work

- Promotion is restricted to the existing committed replica list. There is no dynamic ISR removal or staged replica reassignment.
- If no replica proves the complete committed prefix, the controller refuses promotion. This prefers unavailability to promoting a potentially divergent log.
- The current storage engine has no safe in-place truncation of an uncommitted suffix or snapshot/rebootstrap procedure. A replica with a divergent uncommitted tail may need manual rebuild before it can be eligible again.
- Failure detection is based on repeated timeouts and cannot distinguish a crash from a network partition by itself. Fixed-voter controller quorum and authority-lease fencing prevent a minority/stale broker from continuing writes, but peer traffic is still plaintext and unauthenticated. Keep peer/admin listeners on trusted private networks.
- The authority lease is a bounded fail-closed gate, not a replacement for a complete production Raft implementation with proven timing assumptions. Heartbeat/election settings must be configured consistently on all voters.
- Controller leader election and partition leader promotion are different operations. The controller coordinates metadata; a partition leader serves a specific data partition.
- The CLI's metadata refresh/retry is intentionally bounded to one retry and keyed produce only. A network timeout after an append may still leave an uncertain producer outcome; the public protocol does not yet provide a complete idempotent producer session lifecycle.
- No claim of full production readiness should be made until peer TLS/authentication, dynamic ISR/reassignment, divergent-replica rebootstrap, full client routing, and broader network/disk chaos testing are completed.
