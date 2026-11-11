# ADR 0004: Use versioned length-prefixed binary frames over TCP

- Status: Accepted
- Scope: Client and inter-broker protocol

## Context

TCP is a byte stream. It does not preserve application write boundaries, so a receiver cannot assume one read equals one request.

## Decision

Frame messages with an explicit length prefix and encode API/version/correlation information in a binary envelope. Validate sizes before allocating or decoding payloads.

## Consequences

- Partial reads and multiple frames per connection must be handled.
- Protocol versions and API versions need compatibility tests.
- Maximum frame limits are part of the security boundary.
- TLS and authentication must wrap the actual client and inter-broker listeners, not just exist as helper packages.
