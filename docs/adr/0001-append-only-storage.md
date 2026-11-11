# ADR 0001: Use an append-only segmented log

- Status: Accepted
- Scope: Storage engine

## Context

Message brokers need ordered writes, offset-based reads, replay, and predictable crash recovery. Updating arbitrary records in place would complicate ordering and recovery.

## Decision

Represent each partition log as append-only segments. Store record metadata with each value and maintain sparse offset/time indexes for lookup. Rotate segments after a configured size and apply retention by removing eligible old segments.

## Consequences

- Sequential append is the primary write path.
- Offsets are stable identifiers within the retained log history.
- Indexes can be rebuilt/validated from log data.
- Retention changes the earliest available offset; consumers must handle offset-before-start.
- Segment format evolution requires explicit versioning/migration planning.
- A checksum is still required for robust detection of same-length payload corruption.
