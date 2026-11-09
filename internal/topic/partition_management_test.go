package topic

import (
	"errors"
	"os"
	"sync"
	"testing"
)

func TestAddPartitionsAssignsContiguousIDs(t *testing.T) {
	registry := NewRegistry()

	topic, err := registry.CreateWithPartitionCount("orders", 2)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}

	if err := topic.AddPartitions(3); err != nil {
		t.Fatalf("AddPartitions(3) error: %v", err)
	}

	if got := topic.PartitionCount(); got != 5 {
		t.Fatalf("PartitionCount() = %d, want 5", got)
	}

	for id := uint32(0); id < 5; id++ {
		partition, err := topic.Partition(id)
		if err != nil {
			t.Fatalf("Partition(%d) error: %v", id, err)
		}

		if partition.ID() != id {
			t.Errorf("Partition(%d).ID() = %d", id, partition.ID())
		}

		if partition.TopicName() != "orders" {
			t.Errorf(
				"Partition(%d).TopicName() = %q, want orders",
				id,
				partition.TopicName(),
			)
		}
	}
}

func TestAddPartitionsRejectsInvalidRequests(t *testing.T) {
	registry := NewRegistry()

	topic, err := registry.CreateWithPartitionCount("orders", 2)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		target     *Topic
		additional uint32
		want       error
	}{
		{
			name:       "zero partitions",
			target:     topic,
			additional: 0,
			want:       ErrInvalidPartitionCount,
		},
		{
			name:       "nil topic",
			target:     nil,
			additional: 1,
			want:       ErrInvalidPartitionCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.target.AddPartitions(tt.additional)

			if !errors.Is(err, tt.want) {
				t.Fatalf("AddPartitions() error = %v, want %v", err, tt.want)
			}
		})
	}

	if got := topic.PartitionCount(); got != 2 {
		t.Fatalf("PartitionCount() = %d after rejected requests, want 2", got)
	}
}

func TestAddPartitionsWhileLogsAreOpen(t *testing.T) {
	rootDir := t.TempDir()
	topic := newTopic("orders", 1)

	if err := topic.OpenLogs(rootDir, 1024); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}
	defer topic.CloseLogs()

	// Verify the original partition continues to work.
	partition0, err := topic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := partition0.Append([]byte("existing-message")); err != nil {
		t.Fatalf("Partition(0).Append() error: %v", err)
	}

	// Expand the topic while its logs are already open.
	if err := topic.AddPartitions(2); err != nil {
		t.Fatalf("AddPartitions(2) error: %v", err)
	}

	if got := topic.PartitionCount(); got != 3 {
		t.Fatalf("PartitionCount() = %d, want 3", got)
	}

	// New partitions must have their logs opened automatically.
	partition2, err := topic.Partition(2)
	if err != nil {
		t.Fatalf("Partition(2) error: %v", err)
	}

	nextOffset, err := partition2.NextOffset()
	if err != nil {
		t.Fatalf("Partition(2).NextOffset() error: %v", err)
	}

	if nextOffset != 0 {
		t.Fatalf("new partition offset = %d, want 0", nextOffset)
	}

	offset, err := partition2.Append([]byte("new-partition-message"))
	if err != nil {
		t.Fatalf("Partition(2).Append() error: %v", err)
	}

	if offset != 0 {
		t.Fatalf("Partition(2) first offset = %d, want 0", offset)
	}

	// Existing data must remain intact.
	got, err := partition0.Read(0)
	if err != nil {
		t.Fatalf("Partition(0).Read(0) error: %v", err)
	}

	if string(got) != "existing-message" {
		t.Fatalf("Partition(0).Read(0) = %q", got)
	}
}

func TestAddPartitionsWhileLogsAreClosed(t *testing.T) {
	rootDir := t.TempDir()
	topic := newTopic("orders", 1)

	if err := topic.OpenLogs(rootDir, 1024); err != nil {
		t.Fatalf("first OpenLogs() error: %v", err)
	}

	if err := topic.CloseLogs(); err != nil {
		t.Fatalf("CloseLogs() error: %v", err)
	}

	if err := topic.AddPartitions(2); err != nil {
		t.Fatalf("AddPartitions(2) error: %v", err)
	}

	if err := topic.OpenLogs(rootDir, 1024); err != nil {
		t.Fatalf("second OpenLogs() error: %v", err)
	}
	defer topic.CloseLogs()

	partition2, err := topic.Partition(2)
	if err != nil {
		t.Fatal(err)
	}

	offset, err := partition2.Append([]byte("created-after-reopen"))
	if err != nil {
		t.Fatalf("Partition(2).Append() error: %v", err)
	}

	if offset != 0 {
		t.Fatalf("Partition(2) first offset = %d, want 0", offset)
	}
}

func TestAddPartitionsRollsBackOnLogOpenFailure(t *testing.T) {
	rootDir := t.TempDir()
	topic := newTopic("orders", 1)

	if err := topic.OpenLogs(rootDir, 1024); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}
	defer topic.CloseLogs()

	// Block the storage path for partition 2.
	// Partition 1 will open first; partition 2 will fail.
	blocker := partitionLogDirectory(rootDir, "orders", 2)

	if err := os.WriteFile(blocker, []byte("not a directory"), 0644); err != nil {
		t.Fatalf("WriteFile(blocker) error: %v", err)
	}

	err := topic.AddPartitions(2)
	if err == nil {
		t.Fatal("AddPartitions(2) succeeded; want log-open failure")
	}

	if got := topic.PartitionCount(); got != 1 {
		t.Fatalf(
			"PartitionCount() = %d after failed expansion, want 1",
			got,
		)
	}

	if _, err := topic.Partition(1); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("Partition(1) error = %v, want ErrPartitionNotFound", err)
	}

	// The original partition must remain usable.
	partition0, err := topic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := partition0.Append([]byte("still-available")); err != nil {
		t.Fatalf("original partition Append() error: %v", err)
	}

	// Remove the obstruction and retry expansion.
	if err := os.Remove(blocker); err != nil {
		t.Fatalf("Remove(blocker) error: %v", err)
	}

	if err := topic.AddPartitions(2); err != nil {
		t.Fatalf("AddPartitions(2) retry error: %v", err)
	}

	if got := topic.PartitionCount(); got != 3 {
		t.Fatalf("PartitionCount() after retry = %d, want 3", got)
	}
}

func TestPartitionCollectionSupportsConcurrentExpansionAndReads(t *testing.T) {
	topic := newTopic("orders", 1)

	const readers = 4
	const additions = 25

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < readers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
					_ = topic.PartitionCount()
					_ = topic.Partitions()

					if _, err := topic.Partition(0); err != nil {
						t.Errorf("Partition(0) error during expansion: %v", err)
						return
					}
				}
			}
		}()
	}

	for i := 0; i < additions; i++ {
		if err := topic.AddPartitions(1); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("AddPartitions(1) error: %v", err)
		}
	}

	close(stop)
	wg.Wait()

	if got := topic.PartitionCount(); got != 1+additions {
		t.Fatalf(
			"PartitionCount() = %d, want %d",
			got,
			1+additions,
		)
	}
}
