package broker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func TestNewPartitionAssignment(t *testing.T) {
	registry := topic.NewRegistry()

	orders, err := registry.CreateWithPartitionCount("orders", 4)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}

	assignment, err := NewPartitionAssignment(orders)
	if err != nil {
		t.Fatalf("NewPartitionAssignment() error: %v", err)
	}

	if assignment == nil {
		t.Fatal("NewPartitionAssignment() returned nil")
	}
}

func TestNewPartitionAssignmentRejectsNilTopic(t *testing.T) {
	_, err := NewPartitionAssignment(nil)

	if !errors.Is(err, ErrInvalidPartitionAssignment) {
		t.Fatalf(
			"NewPartitionAssignment(nil) error = %v, want ErrInvalidPartitionAssignment",
			err,
		)
	}
}

func TestNewPartitionAssignmentRejectsZeroPartitions(t *testing.T) {
	emptyTopic := &topic.Topic{}

	_, err := NewPartitionAssignment(emptyTopic)

	if !errors.Is(err, ErrInvalidPartitionCount) {
		t.Fatalf(
			"NewPartitionAssignment() error = %v, want ErrInvalidPartitionCount",
			err,
		)
	}
}

func TestPartitionAssignmentUsesKeyBasedRouting(t *testing.T) {
	registry := topic.NewRegistry()

	orders, err := registry.CreateWithPartitionCount("orders", 4)
	if err != nil {
		t.Fatal(err)
	}

	assignment, err := NewPartitionAssignment(orders)
	if err != nil {
		t.Fatal(err)
	}

	first, err := assignment.Assign([]byte("order-1042"))
	if err != nil {
		t.Fatalf("first Assign() error: %v", err)
	}

	for i := 0; i < 100; i++ {
		got, err := assignment.Assign([]byte("order-1042"))
		if err != nil {
			t.Fatalf("Assign() error: %v", err)
		}

		if got.ID() != first.ID() {
			t.Fatalf(
				"same key assigned to different partitions: %d and %d",
				first.ID(),
				got.ID(),
			)
		}
	}
}

func TestPartitionAssignmentUsesRoundRobinRouting(t *testing.T) {
	registry := topic.NewRegistry()

	orders, err := registry.CreateWithPartitionCount("orders", 3)
	if err != nil {
		t.Fatal(err)
	}

	assignment, err := NewPartitionAssignment(orders)
	if err != nil {
		t.Fatal(err)
	}

	want := []uint32{0, 1, 2, 0, 1, 2}

	for i, expected := range want {
		got, err := assignment.Assign(nil)
		if err != nil {
			t.Fatalf("Assign(nil) error: %v", err)
		}

		if got.ID() != expected {
			t.Fatalf(
				"call %d assigned partition %d, want %d",
				i,
				got.ID(),
				expected,
			)
		}
	}
}

func TestPartitionAssignmentEmptyKeyUsesHashing(t *testing.T) {
	registry := topic.NewRegistry()

	orders, err := registry.CreateWithPartitionCount("orders", 3)
	if err != nil {
		t.Fatal(err)
	}

	assignment, err := NewPartitionAssignment(orders)
	if err != nil {
		t.Fatal(err)
	}

	first, err := assignment.Assign([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	second, err := assignment.Assign([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	if first.ID() != second.ID() {
		t.Fatalf(
			"empty key assigned to different partitions: %d and %d",
			first.ID(),
			second.ID(),
		)
	}
}

func TestPartitionAssignmentResolvesCorrectTopic(t *testing.T) {
	registry := topic.NewRegistry()

	orders, err := registry.CreateWithPartitionCount("orders", 3)
	if err != nil {
		t.Fatal(err)
	}

	payments, err := registry.CreateWithPartitionCount("payments", 3)
	if err != nil {
		t.Fatal(err)
	}

	ordersAssignment, err := NewPartitionAssignment(orders)
	if err != nil {
		t.Fatal(err)
	}

	paymentsAssignment, err := NewPartitionAssignment(payments)
	if err != nil {
		t.Fatal(err)
	}

	orderPartition, err := ordersAssignment.Assign(nil)
	if err != nil {
		t.Fatal(err)
	}

	paymentPartition, err := paymentsAssignment.Assign(nil)
	if err != nil {
		t.Fatal(err)
	}

	if orderPartition.ID() != 0 || paymentPartition.ID() != 0 {
		t.Fatal("each topic's first round-robin assignment should be partition 0")
	}

	if orderPartition.TopicName() != "orders" {
		t.Fatalf(
			"order partition belongs to %q, want orders",
			orderPartition.TopicName(),
		)
	}

	if paymentPartition.TopicName() != "payments" {
		t.Fatalf(
			"payment partition belongs to %q, want payments",
			paymentPartition.TopicName(),
		)
	}
}

func TestPartitionAssignmentConcurrentRoundRobin(t *testing.T) {
	registry := topic.NewRegistry()

	orders, err := registry.CreateWithPartitionCount("orders", 3)
	if err != nil {
		t.Fatal(err)
	}

	assignment, err := NewPartitionAssignment(orders)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 300

	var counts [3]atomic.Uint64
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			partition, err := assignment.Assign(nil)
			if err != nil {
				errs <- err
				return
			}

			counts[partition.ID()].Add(1)
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent Assign() error: %v", err)
	}

	for id := uint32(0); id < 3; id++ {
		got := counts[id].Load()
		if got != workers/3 {
			t.Errorf(
				"partition %d received %d assignments, want %d",
				id,
				got,
				workers/3,
			)
		}
	}
}

func TestPartitionAssignmentRejectsNilReceiver(t *testing.T) {
	var assignment *PartitionAssignment

	_, err := assignment.Assign([]byte("order-1042"))

	if !errors.Is(err, ErrInvalidPartitionAssignment) {
		t.Fatalf(
			"Assign() error = %v, want ErrInvalidPartitionAssignment",
			err,
		)
	}
}
