package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	manager, err := NewManager(Config{ControllerIDs: []string{"controller-a", "controller-b", "controller-c"}, BootstrapLeader: "controller-a"})
	if err != nil {
		t.Fatal(err)
	}
	for i, brokerID := range []string{"a", "b", "c", "d"} {
		address := fmt.Sprintf("127.0.0.1:%d", 9001+i)
		if err := manager.RegisterBroker(controller.Broker{ID: brokerID, Address: address}); err != nil {
			t.Fatal(err)
		}
	}
	return manager
}

func TestDeterministicPartitionPlacementAndRouting(t *testing.T) {
	manager := newTestManager(t)
	assignments, err := manager.CreateTopic("orders", 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 4 {
		t.Fatalf("got %d assignments, want 4", len(assignments))
	}
	wantLeaders := []string{"a", "b", "c", "d"}
	for i, assignment := range assignments {
		if assignment.Leader != wantLeaders[i] {
			t.Fatalf("partition %d leader = %q, want %q", i, assignment.Leader, wantLeaders[i])
		}
		if len(assignment.Replicas) != 2 {
			t.Fatalf("partition %d replicas = %v", i, assignment.Replicas)
		}
		route, err := manager.ResolveRoute("orders", i)
		if err != nil {
			t.Fatal(err)
		}
		if route.Leader.ID != assignment.Leader || route.Assignment.Key.Partition != i || route.MetadataVersion == 0 {
			t.Fatalf("unexpected route for partition %d: %+v", i, route)
		}
	}
}

func TestPlacementRejectsInvalidReplicationFactorAndDuplicatePartition(t *testing.T) {
	manager := newTestManager(t)
	if _, err := manager.PlacePartition("orders", 0, 5); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatalf("error = %v, want ErrInvalidPlacement", err)
	}
	if _, err := manager.PlacePartition("orders", 0, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.PlacePartition("orders", 0, 2); !errors.Is(err, ErrPartitionAssigned) {
		t.Fatalf("error = %v, want ErrPartitionAssigned", err)
	}
}

func TestCreateTopicRejectsReplicationFactorBeforeAssigningAnything(t *testing.T) {
	manager := newTestManager(t)
	_, err := manager.CreateTopic("orders", 2, 5)
	if !errors.Is(err, ErrInvalidPlacement) {
		t.Fatalf("error = %v, want ErrInvalidPlacement", err)
	}
	metadata, err := manager.Metadata("controller-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.Partitions) != 0 {
		t.Fatalf("unexpected partial assignments: %+v", metadata.Partitions)
	}
}

func TestResolveRouteForUnknownPartition(t *testing.T) {
	manager := newTestManager(t)
	if _, err := manager.ResolveRoute("orders", 9); !errors.Is(err, ErrPartitionUnassigned) {
		t.Fatalf("error = %v, want ErrPartitionUnassigned", err)
	}
}

func TestMetadataHandlerFiltersTopicAndEncodesBrokerAddresses(t *testing.T) {
	manager := newTestManager(t)
	if _, err := manager.CreateTopic("orders", 2, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateTopic("payments", 1, 1); err != nil {
		t.Fatal(err)
	}
	handler, err := NewMetadataHandler(manager.ControllerCluster(), "controller-a")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protocol.EncodeMetadataRequest(protocol.MetadataRequest{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := handler.Handle(protocol.Request{APIKey: protocol.APIMETADATA, APIVersion: protocol.MetadataAPIVersion, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.DecodeMetadataResponse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Brokers) != 4 || len(response.Partitions) != 2 {
		t.Fatalf("unexpected metadata response: %+v", response)
	}
	if response.Partitions[0].Topic != "orders" || response.Partitions[1].Topic != "orders" {
		t.Fatalf("topic filter failed: %+v", response.Partitions)
	}
	if response.Brokers[0].Address == "" {
		t.Fatal("broker address missing")
	}
}

func TestMetadataHandlerRejectsWrongAPIAndVersion(t *testing.T) {
	manager := newTestManager(t)
	handler, err := NewMetadataHandler(manager.ControllerCluster(), "controller-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler.Handle(protocol.Request{APIKey: protocol.APICONSUME, APIVersion: 1}); !errors.Is(err, protocol.ErrInvalidRequest) {
		t.Fatalf("wrong API error = %v", err)
	}
	if _, err := handler.Handle(protocol.Request{APIKey: protocol.APIMETADATA, APIVersion: 9}); !errors.Is(err, protocol.ErrUnsupportedVersion) {
		t.Fatalf("wrong version error = %v", err)
	}
}

func TestPlacementRejectsInvalidTopicNames(t *testing.T) {
	manager := newTestManager(t)
	for _, name := range []string{"", ".", "..", "orders/2026", "with space"} {
		if _, err := manager.PlacePartition(name, 0, 1); !errors.Is(err, ErrInvalidPlacement) {
			t.Fatalf("topic %q error = %v, want ErrInvalidPlacement", name, err)
		}
	}
}

func TestCreateTopicRejectsTooManyPartitions(t *testing.T) {
	manager := newTestManager(t)
	if _, err := manager.CreateTopic("orders", MaxTopicPartitions+1, 1); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatalf("error = %v, want ErrInvalidPlacement", err)
	}
}

func TestMetadataHandlerReturnsAllTopicsWhenFilterEmpty(t *testing.T) {
	manager := newTestManager(t)
	if _, err := manager.CreateTopic("orders", 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateTopic("payments", 1, 1); err != nil {
		t.Fatal(err)
	}
	handler, err := NewMetadataHandler(manager.ControllerCluster(), "controller-a")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protocol.EncodeMetadataRequest(protocol.MetadataRequest{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := handler.Handle(protocol.Request{APIKey: protocol.APIMETADATA, APIVersion: protocol.MetadataAPIVersion, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.DecodeMetadataResponse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Partitions) != 2 {
		t.Fatalf("got %d partitions, want 2", len(decoded.Partitions))
	}
}
