# Client API & CLI Reference

`github.com/ARIN00-web/messlessmq/mqclient` provides a high-performance, cluster-aware Go client library for MessLessMQ.

---

## Basic Go Client Usage

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
	mqclient "github.com/ARIN00-web/messlessmq/mqclient"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize cluster-aware client
	c, err := mqclient.New(mqclient.Config{
		Bootstrap:   []string{"127.0.0.1:19092"},
		MetadataTTL: 30 * time.Second,
		MaxRetries:  2,
	})
	if err != nil {
		log.Fatalf("initialize client: %v", err)
	}

	// Keyed produce: routed to partition leader via FNV-1a hash
	result, err := c.Produce(ctx, mqclient.ProduceRequest{
		Topic:   "orders",
		Key:     []byte("customer-7"),
		Value:   []byte("order-payload-123"),
		AckMode: protocol.AckAll,
	})
	if err != nil {
		log.Fatalf("produce: %v", err)
	}
	fmt.Printf("Produced to topic=%s partition=%d offset=%d\n", result.Topic, result.PartitionID, result.Offset)

	// Consume records from partition leader
	records, err := c.Consume(ctx, "orders", result.PartitionID, result.Offset, 10)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}
	for _, record := range records {
		fmt.Printf("Consumed offset=%d key=%s value=%s\n", record.Offset, record.Key, record.Value)
	}
}
```

---

## Endpoint Resolution & Multi-Network Support

When clients run outside Docker (e.g. host development on macOS) or outside Kubernetes, brokers advertise reachable host or cluster endpoints. The client includes built-in support for endpoint translation:

```go
// Custom endpoint resolver hook
c, err := mqclient.New(mqclient.Config{
    Bootstrap: []string{"127.0.0.1:19092"},
    EndpointResolver: func(advertised string, brokerID string) string {
        // Translate or proxy broker addresses if necessary
        return advertised
    },
})
```

`mqclient.ResolveAddressWithBootstrap(advertised, bootstrapAddresses, brokerID)` automatically rewrites container ports and localhost bindings to match host port mappings.

---

## Command-Line Interface (CLI)

The CLI tool (`cmd/client`) provides operators and developers with direct access to cluster capabilities:

### 1. Cluster Metadata Discovery
```bash
go run ./cmd/client metadata -addr 127.0.0.1:19092 -topic orders
```
Outputs controller metadata version, registered broker endpoints, partition assignments, current leaders, and ISR lists.

### 2. Producing Messages
```bash
# Keyed produce with quorum durability (acks=all)
go run ./cmd/client produce -addr 127.0.0.1:19092 -topic orders -key user-1 -value "order-data" -acks all

# Local durable produce (acks=leader)
go run ./cmd/client produce -addr 127.0.0.1:19092 -topic orders -key user-2 -value "quick-data" -acks leader

# Direct produce without client-side auto-routing
go run ./cmd/client produce -addr 127.0.0.1:19093 -topic orders -value "direct-record" -auto-route=false
```

### 3. Consuming Messages
```bash
go run ./cmd/client consume -addr 127.0.0.1:19092 -topic orders -partition 0 -offset 0 -max 20
```

---

## Error Handling & Retry Classifications

The client classifies broker response codes to ensure safe distributed operations:

| Error Code | Constant | Meaning & Behavior |
|:---|:---|:---|
| 0 | `ErrorNone` | Operation completed successfully. |
| 13 | `ErrorNotPartitionLeader` | Partition leadership changed (failover). Safe to refresh metadata and retry. |
| 14 | `ErrorInsufficientISR` | Active ISR count is below `MinISR`. Write rejected without advancing high watermark. |
| 15 | `ErrorCommitUncertain` | Leader persisted data, but commit marker failed to reach `MinISR` replicas. **Not retried automatically** to avoid duplicate appends. |
| 16 | `ErrorControllerAuthorityUnavailable` | Controller lease expired or partitioned. Client refreshes metadata and retries. |
| 17 | `ErrorUnauthorized` | Missing or invalid API key (`MESSLESSMQ_CLIENT_API_KEY`). |
| 18 | `ErrorForbidden` | ACL rule violation for the authenticated principal. |
