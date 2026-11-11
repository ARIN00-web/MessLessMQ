# MessLessMQ — Interview & Resume Preparation

## Resume bullets (accurate, project-stage wording)

- Built a message-broker prototype in Go with a length-prefixed binary TCP protocol, request dispatch, append-only segmented storage, sparse indexes, retention, and crash-recovery tests.
- Implemented producer/consumer primitives including batching, offset management, consumer-group assignment, delivery semantics, and idempotency experiments.
- Modeled partition replication with leader/follower roles, ISR membership, leader epochs, high-watermark-based committed reads, and replica catch-up.
- Built a Raft-style in-memory metadata-controller model with voting, log matching, majority commit, and recovery tests.
- Added reusable operational components for metrics, structured logging, authentication/ACLs, TLS configuration, rate limiting, graceful draining, and performance benchmarks.
- Validated the repository with Go unit tests, the race detector, `go vet`, package coverage, and repeatable microbenchmarks.

Use verbs such as *implemented*, *modeled*, and *tested*. Do not claim production-grade Raft, exactly-once transactions across external systems, or a fully operational three-broker replicated cluster until those paths are integrated and tested across processes.

## Core interview questions

1. Why is TCP a byte stream, and how does length-prefix framing solve message boundaries?
2. What is the difference between a message offset and a consumer's committed offset?
3. Why is an append-only log useful for message storage?
4. What is the difference between flushing a buffer and syncing durable storage?
5. Explain at-most-once, at-least-once, and effectively-once processing.
6. Why can at-least-once delivery produce duplicates?
7. What does an idempotent producer protect against, and what does it not protect against?
8. What is an ISR, and how does `MinISR` affect availability and durability?
9. Why is the high watermark exclusive?
10. Why does a follower need to catch up before rejoining ISR?
11. What is a leader epoch, and how does it fence stale leaders?
12. Why does Raft require a majority to commit?
13. How does Raft use terms and log matching to prevent conflicting committed histories?
14. What is the difference between controller leadership and partition leadership?
15. Why must cluster metadata and controller term/vote state survive restart?
16. What can a TCP readiness probe tell you, and what can it not tell you?
17. Why are race tests and chaos tests different?
18. How would you measure p99 latency without confusing it with throughput?
19. Why can high-cardinality metrics overload an observability backend?
20. What would you integrate next before calling this broker production-ready?

## Suggested architecture explanation

Start with the data path: client frame → protocol decode → dispatcher → partition append → durability acknowledgement. Then explain consumption and offsets. Next describe replication and high watermark. Finally separate the metadata control plane (controller quorum and assignments) from partition leaders. Finish with operational gaps honestly: persistent controller state, live inter-broker replication, runtime integration, checksums, and multi-process chaos testing.
