package broker

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/topic"
)

// Creates a topic with persistent partition logs and its assignment component.
func newOrderingTestAssignment(
	t *testing.T,
	partitionCount uint32,
) (*topic.Topic, *PartitionAssignment) {
	t.Helper()

	registry := topic.NewRegistry()

	testTopic, err := registry.CreateWithPartitionCount(
		"orders",
		partitionCount,
	)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}

	if err := testTopic.OpenLogs(t.TempDir(), 1024*1024); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}

	t.Cleanup(func() {
		if err := testTopic.CloseLogs(); err != nil {
			t.Errorf("CloseLogs() error: %v", err)
		}
	})

	assignment, err := NewPartitionAssignment(testTopic)
	if err != nil {
		t.Fatalf("NewPartitionAssignment() error: %v", err)
	}

	return testTopic, assignment
}

// Sequential messages with the same key should reach the same partition
// and receive increasing offsets.
func TestPartitionAssignmentAppendPreservesSequentialKeyOrder(t *testing.T) {
	testTopic, assignment := newOrderingTestAssignment(t, 4)

	messages := [][]byte{
		[]byte("order-created"),
		[]byte("order-confirmed"),
		[]byte("order-packed"),
		[]byte("order-shipped"),
		[]byte("order-delivered"),
	}

	const key = "customer-42"

	var assignedPartition uint32

	for i, message := range messages {
		result, err := assignment.Append([]byte(key), message)
		if err != nil {
			t.Fatalf("Append(%q) error: %v", message, err)
		}

		if i == 0 {
			assignedPartition = result.PartitionID
		}

		if result.PartitionID != assignedPartition {
			t.Fatalf(
				"message %d assigned to partition %d, want %d",
				i,
				result.PartitionID,
				assignedPartition,
			)
		}

		if result.Offset != uint64(i) {
			t.Fatalf(
				"message %d offset = %d, want %d",
				i,
				result.Offset,
				i,
			)
		}
	}

	partition, err := testTopic.Partition(assignedPartition)
	if err != nil {
		t.Fatalf("Partition(%d) error: %v", assignedPartition, err)
	}

	for i, want := range messages {
		got, err := partition.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) error: %v", i, err)
		}

		if string(got) != string(want) {
			t.Errorf(
				"Read(%d) = %q, want %q",
				i,
				got,
				want,
			)
		}
	}
}

// Round-robin assignment should use separate offset sequences
// for separate partitions.
func TestPartitionAssignmentAppendRoundRobinKeepsIndependentOffsets(t *testing.T) {
	testTopic, assignment := newOrderingTestAssignment(t, 3)

	for expectedPartition := uint32(0); expectedPartition < 3; expectedPartition++ {
		result, err := assignment.Append(nil, []byte("message"))
		if err != nil {
			t.Fatalf("Append(nil) error: %v", err)
		}

		if result.PartitionID != expectedPartition {
			t.Fatalf(
				"PartitionID = %d, want %d",
				result.PartitionID,
				expectedPartition,
			)
		}

		if result.Offset != 0 {
			t.Fatalf(
				"first offset for partition %d = %d, want 0",
				expectedPartition,
				result.Offset,
			)
		}
	}

	for id := uint32(0); id < 3; id++ {
		partition, err := testTopic.Partition(id)
		if err != nil {
			t.Fatal(err)
		}

		nextOffset, err := partition.NextOffset()
		if err != nil {
			t.Fatalf("Partition(%d).NextOffset() error: %v", id, err)
		}

		if nextOffset != 1 {
			t.Errorf(
				"Partition(%d) NextOffset() = %d, want 1",
				id,
				nextOffset,
			)
		}
	}
}

// Concurrent messages with the same key should reach the same partition.
// Their append order is determined by storage serialization, not by
// goroutine start time. Every successful append must receive a unique offset.
func TestPartitionAssignmentConcurrentSameKeyAppendsHaveUniqueOffsets(t *testing.T) {
	testTopic, assignment := newOrderingTestAssignment(t, 4)

	const workers = 64
	const key = "customer-42"

	type result struct {
		offset      uint64
		partitionID uint32
		message     string
	}

	results := make(chan result, workers)
	errs := make(chan error, workers)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			<-start

			message := fmt.Sprintf("event-%d", index)

			appendResult, err := assignment.Append(
				[]byte(key),
				[]byte(message),
			)
			if err != nil {
				errs <- err
				return
			}

			results <- result{
				offset:      appendResult.Offset,
				partitionID: appendResult.PartitionID,
				message:     message,
			}
		}(i)
	}

	close(start)
	wg.Wait()

	close(results)
	close(errs)

	for err := range errs {
		t.Errorf("concurrent Append() error: %v", err)
	}

	byOffset := make(map[uint64]string, workers)
	var expectedPartition uint32
	first := true

	for item := range results {
		if first {
			expectedPartition = item.partitionID
			first = false
		}

		if item.partitionID != expectedPartition {
			t.Errorf(
				"same key assigned to partitions %d and %d",
				expectedPartition,
				item.partitionID,
			)
		}

		if _, exists := byOffset[item.offset]; exists {
			t.Errorf("duplicate offset %d", item.offset)
		}

		byOffset[item.offset] = item.message
	}

	if len(byOffset) != workers {
		t.Fatalf(
			"successful appends = %d, want %d",
			len(byOffset),
			workers,
		)
	}

	partition, err := testTopic.Partition(expectedPartition)
	if err != nil {
		t.Fatal(err)
	}

	for offset := uint64(0); offset < workers; offset++ {
		want, exists := byOffset[offset]
		if !exists {
			t.Fatalf("missing offset %d", offset)
		}

		got, err := partition.Read(offset)
		if err != nil {
			t.Fatalf("Read(%d) error: %v", offset, err)
		}

		if string(got) != want {
			t.Errorf(
				"Read(%d) = %q, want %q",
				offset,
				got,
				want,
			)
		}
	}
}
