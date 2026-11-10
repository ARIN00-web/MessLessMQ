package cluster

import (
	"errors"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func TestClientRouterLoadsAndResolvesLeader(t *testing.T) {
	router := NewClientRouter()
	if _, err := router.Resolve("orders", 0); !errors.Is(err, ErrMetadataNotLoaded) {
		t.Fatalf("error = %v, want ErrMetadataNotLoaded", err)
	}
	snapshot := protocol.MetadataResponse{
		Version:    8,
		Brokers:    []protocol.BrokerMetadata{{ID: "b1", Address: "127.0.0.1:9001"}, {ID: "b2", Address: "127.0.0.1:9002"}},
		Partitions: []protocol.PartitionMetadata{{Topic: "orders", PartitionID: 0, Replicas: []string{"b1", "b2"}, Leader: "b2", LeaderEpoch: 3}},
	}
	if err := router.Update(snapshot); err != nil {
		t.Fatal(err)
	}
	route, err := router.Resolve("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if route.Broker.ID != "b2" || route.Broker.Address != "127.0.0.1:9002" || route.MetadataVersion != 8 {
		t.Fatalf("route = %+v", route)
	}
}

func TestClientRouterRejectsStaleAndInvalidMetadata(t *testing.T) {
	router := NewClientRouter()
	newer := protocol.MetadataResponse{Version: 2, Brokers: []protocol.BrokerMetadata{{ID: "b1", Address: "127.0.0.1:1"}}}
	if err := router.Update(newer); err != nil {
		t.Fatal(err)
	}
	if err := router.Update(protocol.MetadataResponse{Version: 1}); !errors.Is(err, ErrStaleMetadata) {
		t.Fatalf("stale error = %v", err)
	}
	invalid := protocol.MetadataResponse{Version: 3, Partitions: []protocol.PartitionMetadata{{Topic: "orders", Replicas: []string{"missing"}, Leader: "missing", LeaderEpoch: 1}}}
	if err := router.Update(invalid); !errors.Is(err, protocol.ErrInvalidMetadataPayload) {
		t.Fatalf("invalid metadata error = %v", err)
	}
	if version, ok := router.MetadataVersion(); !ok || version != 2 {
		t.Fatalf("cached version = %d, loaded=%v", version, ok)
	}
}

func TestClientRouterCopiesMetadata(t *testing.T) {
	router := NewClientRouter()
	snapshot := protocol.MetadataResponse{
		Version:    1,
		Brokers:    []protocol.BrokerMetadata{{ID: "b1", Address: "127.0.0.1:9001"}},
		Partitions: []protocol.PartitionMetadata{{Topic: "orders", Replicas: []string{"b1"}, Leader: "b1", LeaderEpoch: 1}},
	}
	if err := router.Update(snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Brokers[0].Address = "changed"
	snapshot.Partitions[0].Replicas[0] = "changed"
	route, err := router.Resolve("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if route.Broker.Address != "127.0.0.1:9001" || route.Partition.Replicas[0] != "b1" {
		t.Fatalf("cache was mutated by caller: %+v", route)
	}
}

func TestClientRouterRouteNotFoundAndMissingBroker(t *testing.T) {
	router := NewClientRouter()
	if err := router.Update(protocol.MetadataResponse{Version: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Resolve("orders", 0); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("missing route error = %v", err)
	}
}

func TestClientRouterRejectsPartitionWhoseLeaderBrokerIsMissing(t *testing.T) {
	router := NewClientRouter()
	router.metadata = &protocol.MetadataResponse{
		Version:    1,
		Brokers:    []protocol.BrokerMetadata{{ID: "b1", Address: "127.0.0.1:9001"}},
		Partitions: []protocol.PartitionMetadata{{Topic: "orders", PartitionID: 0, Replicas: []string{"b1", "b2"}, Leader: "b2", LeaderEpoch: 1}},
	}
	if _, err := router.Resolve("orders", 0); !errors.Is(err, ErrBrokerUnavailable) {
		t.Fatalf("error = %v, want ErrBrokerUnavailable", err)
	}
}
