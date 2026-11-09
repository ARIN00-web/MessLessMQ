package topic

import (
	"errors"
	"testing"
)

func TestCreateTopicWithPartitionCount(t *testing.T) {
	registry := NewRegistry()

	topic, err := registry.CreateWithPartitionCount("orders", 3)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}

	if topic.Name() != "orders" {
		t.Fatalf("Name() = %q, want orders", topic.Name())
	}

	if topic.PartitionCount() != 3 {
		t.Fatalf(
			"PartitionCount() = %d, want 3",
			topic.PartitionCount(),
		)
	}

	partitions := topic.Partitions()

	for i, partition := range partitions {
		if partition.ID() != uint32(i) {
			t.Errorf(
				"partition at index %d has ID %d",
				i,
				partition.ID(),
			)
		}

		if partition.TopicName() != "orders" {
			t.Errorf(
				"partition topic name = %q, want orders",
				partition.TopicName(),
			)
		}
	}
}

func TestCreateTopicDefaultsToOnePartition(t *testing.T) {
	registry := NewRegistry()

	topic, err := registry.Create("orders")
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if topic.PartitionCount() != 1 {
		t.Fatalf(
			"PartitionCount() = %d, want 1",
			topic.PartitionCount(),
		)
	}

	partition, err := topic.Partition(0)
	if err != nil {
		t.Fatalf("Partition(0) error: %v", err)
	}

	if partition.ID() != 0 {
		t.Fatalf("partition ID = %d, want 0", partition.ID())
	}
}

func TestCreateTopicRejectsZeroPartitions(t *testing.T) {
	registry := NewRegistry()

	_, err := registry.CreateWithPartitionCount("orders", 0)
	if !errors.Is(err, ErrInvalidPartitionCount) {
		t.Fatalf(
			"error = %v, want ErrInvalidPartitionCount",
			err,
		)
	}

	if _, err := registry.Get("orders"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("topic should not have been registered: %v", err)
	}
}

func TestPartitionLookup(t *testing.T) {
	registry := NewRegistry()

	topic, err := registry.CreateWithPartitionCount("orders", 4)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}

	partition, err := topic.Partition(2)
	if err != nil {
		t.Fatalf("Partition(2) error: %v", err)
	}

	if partition.ID() != 2 {
		t.Fatalf("ID() = %d, want 2", partition.ID())
	}

	if partition.TopicName() != "orders" {
		t.Fatalf(
			"TopicName() = %q, want orders",
			partition.TopicName(),
		)
	}
}

func TestPartitionLookupRejectsUnknownID(t *testing.T) {
	registry := NewRegistry()

	topic, err := registry.CreateWithPartitionCount("orders", 3)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}

	_, err = topic.Partition(3)
	if !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf(
			"Partition(3) error = %v, want ErrPartitionNotFound",
			err,
		)
	}
}

func TestPartitionIDsAreScopedToTheirTopics(t *testing.T) {
	registry := NewRegistry()

	orders, err := registry.CreateWithPartitionCount("orders", 2)
	if err != nil {
		t.Fatalf("Create(orders) error: %v", err)
	}

	payments, err := registry.CreateWithPartitionCount("payments", 2)
	if err != nil {
		t.Fatalf("Create(payments) error: %v", err)
	}

	orderPartition, err := orders.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	paymentPartition, err := payments.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	if orderPartition.ID() != paymentPartition.ID() {
		t.Fatal("expected both partition IDs to be zero")
	}

	if orderPartition.TopicName() == paymentPartition.TopicName() {
		t.Fatal("partitions should belong to different topics")
	}
}

func TestPartitionsReturnsIndependentSlice(t *testing.T) {
	registry := NewRegistry()

	topic, err := registry.CreateWithPartitionCount("orders", 3)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}

	partitions := topic.Partitions()

	// Modify the returned slice.
	partitions[0] = nil
	partitions = append(partitions, nil)

	// The topic's internal partition collection must remain unchanged.
	if topic.PartitionCount() != 3 {
		t.Fatalf(
			"PartitionCount() = %d, want 3",
			topic.PartitionCount(),
		)
	}

	partition, err := topic.Partition(0)
	if err != nil {
		t.Fatalf("Partition(0) error: %v", err)
	}

	if partition == nil || partition.ID() != 0 {
		t.Fatal("internal partition collection was unexpectedly modified")
	}
}

func TestNilTopicMethods(t *testing.T) {
	var topic *Topic

	if topic.Name() != "" {
		t.Fatal("nil Topic Name() should return an empty string")
	}

	if topic.PartitionCount() != 0 {
		t.Fatal("nil Topic PartitionCount() should return zero")
	}

	if topic.Partitions() != nil {
		t.Fatal("nil Topic Partitions() should return nil")
	}

	if _, err := topic.Partition(0); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("nil Topic Partition() error = %v", err)
	}
}
