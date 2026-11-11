# ADR 0006: Compose a Persistent Single-Broker Runtime Before Multi-Process Cluster Wiring

- Status: Accepted
- Date: 2026-11-11

## Context

The repository had separately tested protocol, storage, topic, consumer, replication, controller, cluster, security, observability, and operations packages. The executable did not compose those pieces: PRODUCE was a placeholder, the dispatcher had no persistent shared topic registry, and startup did not recover a topic catalog.

## Decision

Introduce `internal/server` as the lifecycle composition root for a persistent single-broker runtime. It loads a durable topic catalog, opens local partition logs, constructs a dispatcher over one shared registry, handles PRODUCE/CONSUME/METADATA requests over the existing TCP protocol, applies request limits and metrics, optionally exposes admin endpoints and TLS/mTLS, and drains/ closes storage on shutdown.

Message keys are stored with values in a versioned CRC32 envelope. Consume API v2 returns key and value while consume API v1 retains the older value-only encoding.

## Consequences

- The repository now has a real single-node produce/consume path and restart test rather than only disconnected packages.
- Topic definitions and partition logs survive local process restart when the same data directory is reused.
- `acks=leader` synchronizes local storage before acknowledgement. `acks=all` is also local-only while the runtime has one local replica.
- This decision intentionally does not claim a distributed cluster. The controller, cluster metadata, and replication packages are not connected to this runtime across processes.
- The next ADR should describe the composition of a real multi-process controller quorum and remote replication transport after those behaviors are implemented and tested.
