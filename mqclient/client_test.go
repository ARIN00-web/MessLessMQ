package client

import (
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func testClient(t *testing.T) *Client {
	t.Helper()
	c, err := New(Config{Bootstrap: []string{"127.0.0.1:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func testMetadata() protocol.MetadataResponse {
	return protocol.MetadataResponse{Version: 1, Brokers: []protocol.BrokerMetadata{{ID: "b1", Address: "a1"}, {ID: "b2", Address: "a2"}}, Partitions: []protocol.PartitionMetadata{{Topic: "orders", PartitionID: 0, Replicas: []string{"b1"}, Leader: "b1", LeaderEpoch: 1}, {Topic: "orders", PartitionID: 1, Replicas: []string{"b2"}, Leader: "b2", LeaderEpoch: 1}}}
}

func TestUnkeyedRoutingRoundRobinsPartitions(t *testing.T) {
	c := testClient(t)
	metadata := testMetadata()
	first, _, err := c.selectRoute(metadata, "orders", nil)
	if err != nil || first != 0 {
		t.Fatalf("first route = %d, %v", first, err)
	}
	second, _, err := c.selectRoute(metadata, "orders", nil)
	if err != nil || second != 1 {
		t.Fatalf("second route = %d, %v", second, err)
	}
}

func TestKeyedRoutingIsStable(t *testing.T) {
	c := testClient(t)
	metadata := testMetadata()
	first, address, err := c.selectRoute(metadata, "orders", []byte("customer-7"))
	if err != nil {
		t.Fatal(err)
	}
	second, secondAddress, err := c.selectRoute(metadata, "orders", []byte("customer-7"))
	if err != nil || first != second || address != secondAddress {
		t.Fatalf("unstable keyed route: %d/%s vs %d/%s (%v)", first, address, second, secondAddress, err)
	}
}

func TestResponseRetryClassificationExcludesUncertainOutcome(t *testing.T) {
	if !isStaleRoute(&ResponseError{Code: protocol.ErrorNotPartitionLeader}) {
		t.Fatal("not-leader should refresh metadata")
	}
	if isStaleRoute(&ResponseError{Code: protocol.ErrorCommitUncertain}) {
		t.Fatal("uncertain append must not be retried")
	}
}
