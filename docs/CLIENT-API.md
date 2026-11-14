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

Keyed records use the broker-compatible FNV-1a partition calculation. Unkeyed records use a client-local round-robin sequence and PRODUCE v2, which carries the selected partition explicitly. Consumers discover the current partition leader before fetch.

The client caches metadata for `MetadataTTL`, refreshes once per configured retry after a not-leader or controller-authority response, and honors context cancellation/deadlines. It does not retry `ErrorCommitUncertain`, storage/network failures after an append, or any other outcome whose durability is unknown. Applications needing retry-safe writes still need an idempotent producer protocol, which is not yet exposed by the broker wire API.
