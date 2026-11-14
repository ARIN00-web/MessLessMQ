# Security

Client listeners support TLS/mTLS through `TLS_CERT_FILE`, `TLS_KEY_FILE`, `TLS_CLIENT_CA_FILE`, and `REQUIRE_CLIENT_CERT`.

Inter-broker traffic can be protected with `PEER_TLS_CERT_FILE`, `PEER_TLS_KEY_FILE`, and `PEER_TLS_CA_FILE`. Set `REQUIRE_PEER_CERT=true` to require mTLS. Peer clients derive the expected TLS server name from the advertised peer host, so certificates must include the DNS name or IP address used in peer addresses.

TLS is opt-in for backward compatibility. A secured cluster must configure identical trust roots and appropriate per-broker certificates on every peer; mixing plaintext and TLS peers is unsupported. Envelope identities and persisted cluster IDs continue to provide application-level consistency checks, but certificate subject-to-broker-ID authorization is not yet implemented.

Set `ADMIN_API_KEY` to protect admin and diagnostic endpoints with `Authorization: Bearer <key>`. Liveness and readiness remain unauthenticated for Kubernetes probes. Keep the key in a secret manager or Kubernetes Secret; the broker never logs it.

Set `CLIENT_API_KEY` to require the binary `AUTH` API v1 before PRODUCE, CONSUME, METADATA, or GROUP requests on a TCP connection. Set `CLIENT_ACL` to semicolon-separated `role:topic:action` entries. Valid actions are `produce`, `consume`, `metadata`, and `group`; topic and action support `*`. The configured key uses the `client` role. When `CLIENT_ACL` is omitted, this role retains backward-compatible wildcard access.
