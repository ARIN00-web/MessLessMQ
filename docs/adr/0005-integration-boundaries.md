# ADR 0005: Keep current cluster components modular until runtime integration is verified

- Status: Accepted for the end of this curriculum; revisit before production
- Scope: Broker startup and subsystem composition

## Context

The project was built incrementally. Storage, replication, controller, metadata, security, observability, and operations have their own APIs and tests, but independent package correctness does not prove end-to-end composition.

## Decision

Keep deployment manifests and production test scripts explicit about what the current broker executable does. Do not claim that three broker containers form a replicated cluster until the executable starts with stable broker identity, loads durable state, registers with the controller, serves metadata, routes partition requests, and exchanges replication traffic.

## Consequences

- Current Compose/Kubernetes assets are standalone process/topology labs.
- Their TCP probes establish listener availability only.
- Runtime integration is a distinct engineering milestone.
- The final architecture review records the concrete work required before production claims.
