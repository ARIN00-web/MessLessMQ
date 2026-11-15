# Client API

`github.com/ARIN00-web/messlessmq/mqclient` provides a cluster-aware Go client.

```go
c, err := client.New(client.Config{
    Bootstrap: []string{"127.0.0.1:9092"},
    MetadataTTL: 30 * time.Second,
    MaxRetries: 1,
})
result, err := c.Produce(ctx, client.ProduceRequest{
    Topic: "orders", Key: []byte("customer-7"), Value: []byte("created"),
    AckMode: protocol.AckAll,
})
```

## Routing and retries

Keyed records use the broker-compatible FNV-1a partition calculation. Unkeyed records use a client-local round-robin sequence and PRODUCE v2, which carries the selected partition explicitly. Consumers discover the current partition leader before fetch.

The client caches topic-scoped metadata snapshots for `MetadataTTL`, refreshes after stale-leader/controller-authority errors, and honors context cancellation/deadlines. It retries only these known stale-routing errors. It does not retry `ErrorCommitUncertain`, arbitrary transport errors after a possible append, or other outcomes whose durability is unknown.

## TLS and authentication

The client supports TLS and optional client certificates by passing a `*tls.Config`. If `ServerName` is empty, it derives the hostname from each advertised broker address. The client can also send the configured binary AUTH API key on each newly opened connection.

```go
clientTLS, err := security.LoadClientTLSWithCertificate(
    "./certs/ca.crt", "", "./certs/client.crt", "./certs/client.key",
)
if err != nil { /* handle */ }

c, err := client.New(client.Config{
    Bootstrap: []string{"broker-1.example:9092", "broker-2.example:9092"},
    TLSConfig: clientTLS,
    APIKey: os.Getenv("MESSLESSMQ_CLIENT_API_KEY"),
    MetadataTTL: 30 * time.Second,
    MaxRetries: 1,
})
```

`Client.Request(ctx, address, api, version, payload)` is available for a known broker address and uses the same TLS/AUTH settings. Normal produce/consume calls should use the routed APIs.

## Delivery caution

Applications needing retry-safe writes still need a broker-supported idempotent producer/session lifecycle, which is not yet exposed end-to-end. A timeout after a possible append can have an uncertain result; do not blindly retry it.
