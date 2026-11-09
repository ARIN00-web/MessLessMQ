package topic

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestTopicOpenLogsCreatesIndependentPartitionLogs(t *testing.T) {
	rootDir := t.TempDir()
	registry := NewRegistry()

	topic, err := registry.CreateWithPartitionCount("orders", 3)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}

	if err := topic.OpenLogs(rootDir, 1024); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}
	defer topic.CloseLogs()

	for partitionID := uint32(0); partitionID < 3; partitionID++ {
		partition, err := topic.Partition(partitionID)
		if err != nil {
			t.Fatalf("Partition(%d) error: %v", partitionID, err)
		}

		message := []byte("message-for-partition-" + string(rune('0'+partitionID)))

		offset, err := partition.Append(message)
		if err != nil {
			t.Fatalf("Partition(%d).Append() error: %v", partitionID, err)
		}

		if offset != 0 {
			t.Fatalf(
				"Partition(%d) first offset = %d, want 0",
				partitionID,
				offset,
			)
		}

		got, err := partition.Read(0)
		if err != nil {
			t.Fatalf("Partition(%d).Read(0) error: %v", partitionID, err)
		}

		if string(got) != string(message) {
			t.Fatalf(
				"Partition(%d).Read(0) = %q, want %q",
				partitionID,
				got,
				message,
			)
		}

		dir := partitionLogDirectory(rootDir, "orders", partitionID)
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("partition directory %s: %v", dir, err)
		}
		if !info.IsDir() {
			t.Fatalf("partition path %s is not a directory", dir)
		}
	}
}

func TestTopicOpenLogsReopensPersistedPartitionLogs(t *testing.T) {
	rootDir := t.TempDir()
	registry := NewRegistry()

	firstTopic, err := registry.CreateWithPartitionCount("orders", 2)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}

	if err := firstTopic.OpenLogs(rootDir, 1024); err != nil {
		t.Fatalf("first OpenLogs() error: %v", err)
	}

	firstPartition, err := firstTopic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	offset, err := firstPartition.AppendAt([]byte("persisted-message"), 1234)
	if err != nil {
		t.Fatalf("AppendAt() error: %v", err)
	}
	if offset != 0 {
		t.Fatalf("first offset = %d, want 0", offset)
	}

	offset, err = firstPartition.AppendSync([]byte("durable-message"))
	if err != nil {
		t.Fatalf("AppendSync() error: %v", err)
	}
	if offset != 1 {
		t.Fatalf("second offset = %d, want 1", offset)
	}

	if err := firstTopic.CloseLogs(); err != nil {
		t.Fatalf("first CloseLogs() error: %v", err)
	}

	// Simulate a new in-memory topic registry after a restart.
	restartedRegistry := NewRegistry()

	restartedTopic, err := restartedRegistry.CreateWithPartitionCount("orders", 2)
	if err != nil {
		t.Fatalf("recreated topic error: %v", err)
	}

	if err := restartedTopic.OpenLogs(rootDir, 1024); err != nil {
		t.Fatalf("reopened logs error: %v", err)
	}
	defer restartedTopic.CloseLogs()

	restartedPartition, err := restartedTopic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	nextOffset, err := restartedPartition.NextOffset()
	if err != nil {
		t.Fatalf("NextOffset() error: %v", err)
	}
	if nextOffset != 2 {
		t.Fatalf("NextOffset() = %d, want 2", nextOffset)
	}

	got, timestamp, err := restartedPartition.ReadWithTimestamp(0)
	if err != nil {
		t.Fatalf("ReadWithTimestamp(0) error: %v", err)
	}
	if string(got) != "persisted-message" {
		t.Fatalf("Read(0) = %q, want %q", got, "persisted-message")
	}
	if timestamp != 1234 {
		t.Fatalf("timestamp = %d, want 1234", timestamp)
	}

	// A different partition has its own independent offset sequence.
	otherPartition, err := restartedTopic.Partition(1)
	if err != nil {
		t.Fatal(err)
	}

	otherNextOffset, err := otherPartition.NextOffset()
	if err != nil {
		t.Fatalf("other partition NextOffset() error: %v", err)
	}
	if otherNextOffset != 0 {
		t.Fatalf("other partition NextOffset() = %d, want 0", otherNextOffset)
	}
}

func TestTopicOpenLogsRejectsInvalidConfiguration(t *testing.T) {
	topic := newTopic("orders", 2)

	tests := []struct {
		name            string
		rootDir         string
		maxSegmentBytes int64
	}{
		{
			name:            "empty root directory",
			rootDir:         "",
			maxSegmentBytes: 1024,
		},
		{
			name:            "zero segment size",
			rootDir:         t.TempDir(),
			maxSegmentBytes: 0,
		},
		{
			name:            "negative segment size",
			rootDir:         t.TempDir(),
			maxSegmentBytes: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := topic.OpenLogs(tt.rootDir, tt.maxSegmentBytes)

			if !errors.Is(err, ErrInvalidPartitionLogConfig) {
				t.Fatalf(
					"OpenLogs() error = %v, want ErrInvalidPartitionLogConfig",
					err,
				)
			}
		})
	}
}

func TestTopicOpenLogsRejectsAlreadyOpenLogs(t *testing.T) {
	topic := newTopic("orders", 2)
	rootDir := t.TempDir()

	if err := topic.OpenLogs(rootDir, 1024); err != nil {
		t.Fatalf("first OpenLogs() error: %v", err)
	}
	defer topic.CloseLogs()

	err := topic.OpenLogs(rootDir, 1024)
	if !errors.Is(err, ErrPartitionLogAlreadyOpen) {
		t.Fatalf(
			"second OpenLogs() error = %v, want ErrPartitionLogAlreadyOpen",
			err,
		)
	}

	partition, err := topic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := partition.Append([]byte("still-open")); err != nil {
		t.Fatalf("Append() after rejected second open: %v", err)
	}
}

func TestTopicOpenLogsRollsBackOnPartialFailure(t *testing.T) {
	rootDir := t.TempDir()
	topic := newTopic("orders", 2)

	topicDir := filepath.Join(rootDir, "orders")
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}

	// Partition 1 cannot be opened because its expected directory is a file.
	blocker := partitionLogDirectory(rootDir, "orders", 1)
	if err := os.WriteFile(blocker, []byte("not a directory"), 0644); err != nil {
		t.Fatalf("WriteFile(blocker) error: %v", err)
	}

	err := topic.OpenLogs(rootDir, 1024)
	if err == nil {
		t.Fatal("OpenLogs() succeeded; want partial-open failure")
	}

	// Partition 0 was opened first, so it must have been closed during rollback.
	partition0, err := topic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := partition0.Append([]byte("should-fail")); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Fatalf(
			"Partition(0).Append() error = %v, want ErrPartitionLogNotOpen",
			err,
		)
	}

	// Remove the obstruction and verify that the topic can be opened again.
	if err := os.Remove(blocker); err != nil {
		t.Fatalf("Remove(blocker) error: %v", err)
	}

	if err := topic.OpenLogs(rootDir, 1024); err != nil {
		t.Fatalf("OpenLogs() after removing blocker: %v", err)
	}
	defer topic.CloseLogs()

	if _, err := partition0.Append([]byte("recovered")); err != nil {
		t.Fatalf("Partition(0).Append() after retry: %v", err)
	}
}

func TestPartitionOperationsRequireOpenLog(t *testing.T) {
	topic := newTopic("orders", 1)
	partition, err := topic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := partition.Append([]byte("message")); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Errorf("Append() error = %v", err)
	}

	if _, err := partition.AppendAt([]byte("message"), 1000); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Errorf("AppendAt() error = %v", err)
	}

	if _, err := partition.AppendSync([]byte("message")); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Errorf("AppendSync() error = %v", err)
	}

	if _, err := partition.Read(0); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Errorf("Read() error = %v", err)
	}

	if _, _, err := partition.ReadWithTimestamp(0); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Errorf("ReadWithTimestamp() error = %v", err)
	}

	if _, _, _, err := partition.ReadAtOrAfterTimestamp(1000); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Errorf("ReadAtOrAfterTimestamp() error = %v", err)
	}

	if _, err := partition.NextOffset(); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Errorf("NextOffset() error = %v", err)
	}

	if err := partition.Sync(); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Errorf("Sync() error = %v", err)
	}

	// Closing a partition that has never been opened is safe.
	if err := partition.Close(); err != nil {
		t.Errorf("Close() on unopened partition error: %v", err)
	}
}

func TestTopicCloseLogsIsIdempotent(t *testing.T) {
	topic := newTopic("orders", 2)

	if err := topic.OpenLogs(t.TempDir(), 1024); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}

	if err := topic.CloseLogs(); err != nil {
		t.Fatalf("first CloseLogs() error: %v", err)
	}

	if err := topic.CloseLogs(); err != nil {
		t.Fatalf("second CloseLogs() error: %v", err)
	}

	partition, err := topic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := partition.NextOffset(); !errors.Is(err, ErrPartitionLogNotOpen) {
		t.Fatalf(
			"NextOffset() after CloseLogs() = %v, want ErrPartitionLogNotOpen",
			err,
		)
	}
}

func TestPartitionConcurrentAppends(t *testing.T) {
	topic := newTopic("orders", 1)

	if err := topic.OpenLogs(t.TempDir(), 1024*1024); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}
	defer topic.CloseLogs()

	partition, err := topic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 32

	var wg sync.WaitGroup
	start := make(chan struct{})
	offsets := make(chan uint64, workers)
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(value byte) {
			defer wg.Done()
			<-start

			offset, err := partition.Append([]byte{value})
			if err != nil {
				errs <- err
				return
			}

			offsets <- offset
		}(byte(i))
	}

	close(start)
	wg.Wait()
	close(offsets)
	close(errs)

	for err := range errs {
		t.Errorf("concurrent Append() error: %v", err)
	}

	seen := make([]bool, workers)

	for offset := range offsets {
		if offset >= workers {
			t.Errorf("offset %d is outside expected range", offset)
			continue
		}

		if seen[offset] {
			t.Errorf("duplicate offset %d", offset)
			continue
		}

		seen[offset] = true
	}

	for offset, exists := range seen {
		if !exists {
			t.Errorf("offset %d was not assigned", offset)
		}
	}

	nextOffset, err := partition.NextOffset()
	if err != nil {
		t.Fatalf("NextOffset() error: %v", err)
	}

	if nextOffset != workers {
		t.Fatalf("NextOffset() = %d, want %d", nextOffset, workers)
	}
}
