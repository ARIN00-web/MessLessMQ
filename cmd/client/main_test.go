package main

import (
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func TestPartitionForKeyMirrorsBrokerPartitioner(t *testing.T) {
	metadata := protocol.MetadataResponse{
		Brokers: []protocol.BrokerMetadata{
			{ID: "broker-1", Address: "127.0.0.1:9092"},
			{ID: "broker-2", Address: "127.0.0.1:9093"},
			{ID: "broker-3", Address: "127.0.0.1:9094"},
		},
		Partitions: []protocol.PartitionMetadata{
			{Topic: "orders", PartitionID: 2, Replicas: []string{"broker-3", "broker-1", "broker-2"}, Leader: "broker-3", LeaderEpoch: 1},
			{Topic: "orders", PartitionID: 0, Replicas: []string{"broker-1", "broker-2", "broker-3"}, Leader: "broker-2", LeaderEpoch: 2},
			{Topic: "orders", PartitionID: 1, Replicas: []string{"broker-2", "broker-3", "broker-1"}, Leader: "broker-1", LeaderEpoch: 1},
		},
	}
	selected, err := partitionForKey(metadata, "orders", []byte("customer-7"))
	if err != nil {
		t.Fatal(err)
	}
	if selected.PartitionID != 0 || selected.Leader != "broker-2" {
		t.Fatalf("selected partition = %+v, want orders-0 led by broker-2", selected)
	}
}

func TestPartitionForKeyRejectsMissingKeyOrTopic(t *testing.T) {
	metadata := protocol.MetadataResponse{}
	if _, err := partitionForKey(metadata, "orders", nil); err == nil {
		t.Fatal("empty key unexpectedly allowed for automatic routing")
	}
	if _, err := partitionForKey(metadata, "orders", []byte("customer-7")); err == nil {
		t.Fatal("missing topic metadata unexpectedly allowed")
	}
}
