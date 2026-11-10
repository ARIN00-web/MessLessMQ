package consumer

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/storage"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func createTestTopic(t *testing.T, name string, partitionCount uint32) *topic.Topic {
	t.Helper()
	registry := topic.NewRegistry()
	testTopic, err := registry.CreateWithPartitionCount(name, partitionCount)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}
	return testTopic
}

func openTestTopic(t *testing.T, name string, partitionCount uint32) *topic.Topic {
	t.Helper()
	testTopic := createTestTopic(t, name, partitionCount)
	if err := testTopic.OpenLogs(t.TempDir(), 1024*1024); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}
	t.Cleanup(func() {
		if err := testTopic.CloseLogs(); err != nil {
			t.Errorf("CloseLogs() error: %v", err)
		}
	})
	return testTopic
}

func TestNewConsumerWithOnePartition(t *testing.T) {
	testTopic := createTestTopic(t, "orders", 3)
	partition, err := testTopic.Partition(1)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(partition)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if c.PartitionCount() != 1 {
		t.Fatalf("PartitionCount() = %d, want 1", c.PartitionCount())
	}
	if !c.HasPartition("orders", 1) {
		t.Fatal("consumer should own orders partition 1")
	}
}

func TestNewConsumerWithMultiplePartitions(t *testing.T) {
	orders := createTestTopic(t, "orders", 3)
	payments := createTestTopic(t, "payments", 2)
	orders0, _ := orders.Partition(0)
	orders2, _ := orders.Partition(2)
	payments1, _ := payments.Partition(1)

	c, err := New(orders0, orders2, payments1)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if c.PartitionCount() != 3 {
		t.Fatalf("PartitionCount() = %d, want 3", c.PartitionCount())
	}
	for _, item := range []struct {
		topic string
		id    uint32
	}{{"orders", 0}, {"orders", 2}, {"payments", 1}} {
		if !c.HasPartition(item.topic, item.id) {
			t.Errorf("consumer missing %s partition %d", item.topic, item.id)
		}
	}
}

func TestNewConsumerRejectsNoPartitions(t *testing.T) {
	if _, err := New(); !errors.Is(err, ErrNoPartitions) {
		t.Fatalf("New() error = %v, want ErrNoPartitions", err)
	}
}

func TestNewConsumerRejectsNilPartition(t *testing.T) {
	testTopic := createTestTopic(t, "orders", 2)
	partition, _ := testTopic.Partition(0)
	if _, err := New(partition, nil); !errors.Is(err, ErrNilPartition) {
		t.Fatalf("New() error = %v, want ErrNilPartition", err)
	}
}

func TestNewConsumerRejectsDuplicatePartition(t *testing.T) {
	testTopic := createTestTopic(t, "orders", 2)
	partition, _ := testTopic.Partition(0)
	if _, err := New(partition, partition); !errors.Is(err, ErrDuplicatePartition) {
		t.Fatalf("New() error = %v, want ErrDuplicatePartition", err)
	}
}

func TestNewConsumerRejectsDuplicateLogicalPartition(t *testing.T) {
	firstTopic := createTestTopic(t, "orders", 2)
	secondTopic := createTestTopic(t, "orders", 2)
	firstPartition, _ := firstTopic.Partition(0)
	secondPartition, _ := secondTopic.Partition(0)
	if firstPartition == secondPartition {
		t.Fatal("expected separate partition objects")
	}
	if _, err := New(firstPartition, secondPartition); !errors.Is(err, ErrDuplicatePartition) {
		t.Fatalf("New() error = %v, want ErrDuplicatePartition", err)
	}
}

func TestConsumerGetPartition(t *testing.T) {
	testTopic := createTestTopic(t, "orders", 3)
	partition, _ := testTopic.Partition(2)
	c, err := New(partition)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.GetPartition("orders", 2)
	if err != nil {
		t.Fatalf("GetPartition() error: %v", err)
	}
	if got != partition {
		t.Fatal("GetPartition() returned a different partition")
	}
}

func TestConsumerGetUnassignedPartition(t *testing.T) {
	testTopic := createTestTopic(t, "orders", 3)
	partition, _ := testTopic.Partition(0)
	c, _ := New(partition)
	if _, err := c.GetPartition("orders", 2); !errors.Is(err, ErrPartitionNotAssigned) {
		t.Fatalf("GetPartition() error = %v, want ErrPartitionNotAssigned", err)
	}
}

func TestConsumerPartitionsReturnsCopy(t *testing.T) {
	testTopic := createTestTopic(t, "orders", 2)
	partition0, _ := testTopic.Partition(0)
	partition1, _ := testTopic.Partition(1)
	c, _ := New(partition0, partition1)

	partitions := c.Partitions()
	partitions[0] = nil
	partitions = append(partitions, nil)
	if c.PartitionCount() != 2 || !c.HasPartition("orders", 0) {
		t.Fatal("modifying returned slice changed consumer assignments")
	}
}

func TestMultipleConsumersCanReadSamePartitionLater(t *testing.T) {
	testTopic := createTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	consumerA, err := New(partition)
	if err != nil {
		t.Fatal(err)
	}
	consumerB, err := New(partition)
	if err != nil {
		t.Fatal(err)
	}
	if !consumerA.HasPartition("orders", 0) || !consumerB.HasPartition("orders", 0) {
		t.Fatal("both consumers should be able to hold the same partition reference")
	}
	if consumerA == consumerB {
		t.Fatal("expected independent consumer objects")
	}
}

func TestNilConsumerMethods(t *testing.T) {
	var c *Consumer
	if c.PartitionCount() != 0 || c.Partitions() != nil || c.HasPartition("orders", 0) {
		t.Fatal("nil consumer methods returned unexpected values")
	}
	if _, err := c.GetPartition("orders", 0); !errors.Is(err, ErrPartitionNotAssigned) {
		t.Fatalf("GetPartition() error = %v, want ErrPartitionNotAssigned", err)
	}
	if _, err := c.Position("orders", 0); !errors.Is(err, ErrPartitionNotAssigned) {
		t.Fatalf("Position() error = %v, want ErrPartitionNotAssigned", err)
	}
	if _, err := c.Fetch("orders", 0, 1); !errors.Is(err, ErrPartitionNotAssigned) {
		t.Fatalf("Fetch() error = %v, want ErrPartitionNotAssigned", err)
	}
}

func TestFetchReturnsMessagesInOffsetOrderAndAdvancesPosition(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	messages := []string{"created", "paid", "shipped"}
	for _, message := range messages {
		if _, err := partition.Append([]byte(message)); err != nil {
			t.Fatal(err)
		}
	}

	c, err := New(partition)
	if err != nil {
		t.Fatal(err)
	}

	first, err := c.Fetch("orders", 0, 2)
	if err != nil {
		t.Fatalf("first Fetch() error: %v", err)
	}
	if len(first.Records) != 2 || first.Records[0].Offset != 0 || first.Records[1].Offset != 1 {
		t.Fatalf("first fetch records = %+v", first.Records)
	}
	if string(first.Records[0].Value) != "created" || string(first.Records[1].Value) != "paid" {
		t.Fatalf("first fetch values = %+v", first.Records)
	}
	if first.StartOffset != 0 || first.LogEndOffset != 3 || first.NextOffset != 2 {
		t.Fatalf("first fetch offsets = %+v", first)
	}

	position, err := c.Position("orders", 0)
	if err != nil || position != 2 {
		t.Fatalf("Position() = (%d, %v), want (2, nil)", position, err)
	}

	second, err := c.Fetch("orders", 0, 2)
	if err != nil {
		t.Fatalf("second Fetch() error: %v", err)
	}
	if len(second.Records) != 1 || string(second.Records[0].Value) != "shipped" || second.NextOffset != 3 {
		t.Fatalf("second fetch = %+v", second)
	}
}

func TestFetchAtLogEndReturnsEmptyResult(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	if _, err := partition.Append([]byte("one")); err != nil {
		t.Fatal(err)
	}
	c, _ := New(partition)
	if err := c.ResetToLatest("orders", 0); err != nil {
		t.Fatal(err)
	}
	result, err := c.Fetch("orders", 0, 10)
	if err != nil {
		t.Fatalf("Fetch() at log end error: %v", err)
	}
	if len(result.Records) != 0 || result.NextOffset != 1 {
		t.Fatalf("empty fetch result = %+v", result)
	}
}

func TestFetchRejectsInvalidLimits(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	c, _ := New(partition)
	for _, limit := range []uint32{0, MaxFetchMessages + 1} {
		if _, err := c.Fetch("orders", 0, limit); !errors.Is(err, ErrInvalidFetchLimit) {
			t.Errorf("Fetch(limit=%d) error = %v, want ErrInvalidFetchLimit", limit, err)
		}
	}
}

func TestFetchRejectsUnassignedPartition(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 2)
	partition, _ := testTopic.Partition(0)
	c, _ := New(partition)
	if _, err := c.Fetch("orders", 1, 1); !errors.Is(err, ErrPartitionNotAssigned) {
		t.Fatalf("Fetch() error = %v, want ErrPartitionNotAssigned", err)
	}
}

func TestSetPositionValidatesRetainedRange(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	for i := 0; i < 3; i++ {
		if _, err := partition.Append([]byte(fmt.Sprintf("message-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	c, _ := New(partition)

	if err := c.SetPosition("orders", 0, 2); err != nil {
		t.Fatalf("SetPosition(2) error: %v", err)
	}
	if position, _ := c.Position("orders", 0); position != 2 {
		t.Fatalf("Position() = %d, want 2", position)
	}
	if err := c.SetPosition("orders", 0, 4); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("SetPosition(4) error = %v, want ErrOffsetOutOfRange", err)
	}
}

func TestFetchReportsOffsetBeforeStartAfterRetention(t *testing.T) {
	testTopic := createTestTopic(t, "orders", 1)
	if err := testTopic.OpenLogs(t.TempDir(), 10); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}
	t.Cleanup(func() {
		if err := testTopic.CloseLogs(); err != nil {
			t.Errorf("CloseLogs() error: %v", err)
		}
	})
	partition, _ := testTopic.Partition(0)
	if _, err := partition.AppendAt([]byte("expired"), 1_000); err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendAt([]byte("recent"), time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}

	c, err := New(partition)
	if err != nil {
		t.Fatal(err)
	}
	// Force the read position to zero before retention advances the start.
	if err := c.SetPosition("orders", 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := partition.ApplyRetention(storageRetentionAge()); err != nil {
		t.Fatalf("ApplyRetention() error: %v", err)
	}
	if _, err := c.Fetch("orders", 0, 10); !errors.Is(err, ErrOffsetBeforeStart) {
		t.Fatalf("Fetch() error = %v, want ErrOffsetBeforeStart", err)
	}
}

func storageRetentionAge() storage.RetentionConfig {
	return storage.RetentionConfig{MaxAge: time.Hour}
}

func TestSetPositionRejectsOffsetBeyondEnd(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	c, _ := New(partition)
	if err := c.SetPosition("orders", 0, 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("SetPosition(1) error = %v, want ErrOffsetOutOfRange", err)
	}
}

func TestReplayFromEarlierOffset(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	for _, value := range []string{"zero", "one", "two", "three"} {
		if _, err := partition.Append([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	c, _ := New(partition)
	if _, err := c.Fetch("orders", 0, 3); err != nil {
		t.Fatal(err)
	}

	replayed, err := c.Replay("orders", 0, 1, 2)
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}
	if len(replayed.Records) != 2 || string(replayed.Records[0].Value) != "one" || string(replayed.Records[1].Value) != "two" {
		t.Fatalf("Replay() records = %+v", replayed.Records)
	}
	if replayed.NextOffset != 3 {
		t.Fatalf("Replay() NextOffset = %d, want 3", replayed.NextOffset)
	}
	if position, _ := c.Position("orders", 0); position != 3 {
		t.Fatalf("Position() after replay = %d, want 3", position)
	}
}

func TestResetToEarliestAndLatest(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	for _, value := range []string{"a", "b", "c"} {
		if _, err := partition.Append([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	c, _ := New(partition)

	if err := c.ResetToLatest("orders", 0); err != nil {
		t.Fatal(err)
	}
	if position, _ := c.Position("orders", 0); position != 3 {
		t.Fatalf("latest position = %d, want 3", position)
	}

	if err := c.ResetToEarliest("orders", 0); err != nil {
		t.Fatal(err)
	}
	if position, _ := c.Position("orders", 0); position != 0 {
		t.Fatalf("earliest position = %d, want 0", position)
	}
}

func TestFetchDoesNotAdvancePositionOnError(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	c, _ := New(partition)

	if _, err := c.Fetch("orders", 0, 0); !errors.Is(err, ErrInvalidFetchLimit) {
		t.Fatalf("Fetch() error = %v, want ErrInvalidFetchLimit", err)
	}
	position, err := c.Position("orders", 0)
	if err != nil || position != 0 {
		t.Fatalf("Position() = (%d, %v), want (0, nil)", position, err)
	}
}

func TestConcurrentFetchesFromOneConsumerSerializePositions(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	for i := 0; i < 100; i++ {
		if _, err := partition.Append([]byte(fmt.Sprintf("message-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	c, _ := New(partition)

	const workers = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[uint64]struct{}, 100)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := c.Fetch("orders", 0, 10)
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, record := range result.Records {
				if _, exists := seen[record.Offset]; exists {
					errs <- fmt.Errorf("duplicate fetched offset %d", record.Offset)
					return
				}
				seen[record.Offset] = struct{}{}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if len(seen) != 100 {
		t.Fatalf("fetched unique records = %d, want 100", len(seen))
	}
	if position, _ := c.Position("orders", 0); position != 100 {
		t.Fatalf("final position = %d, want 100", position)
	}
}

func TestConsumerStartsAtEarliestRetainedOffset(t *testing.T) {
	testTopic := createTestTopic(t, "orders", 1)
	if err := testTopic.OpenLogs(t.TempDir(), 10); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}
	defer testTopic.CloseLogs()

	partition, _ := testTopic.Partition(0)
	if _, err := partition.AppendAt([]byte("expired"), 1_000); err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendAt([]byte("recent"), time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := partition.ApplyRetention(storage.RetentionConfig{MaxAge: time.Hour}); err != nil {
		t.Fatal(err)
	}

	c, err := New(partition)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	position, err := c.Position("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if position != 1 {
		t.Fatalf("initial position = %d, want earliest retained offset 1", position)
	}
}

func TestConsumerPositionsAreIndependentPerPartition(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 2)
	partition0, _ := testTopic.Partition(0)
	partition1, _ := testTopic.Partition(1)
	for i := 0; i < 2; i++ {
		if _, err := partition0.Append([]byte(fmt.Sprintf("p0-%d", i))); err != nil {
			t.Fatal(err)
		}
		if _, err := partition1.Append([]byte(fmt.Sprintf("p1-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	c, err := New(partition0, partition1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fetch("orders", 0, 1); err != nil {
		t.Fatal(err)
	}
	p0, _ := c.Position("orders", 0)
	p1, _ := c.Position("orders", 1)
	if p0 != 1 || p1 != 0 {
		t.Fatalf("positions = (%d, %d), want (1, 0)", p0, p1)
	}
}

func TestReplayRejectsOffsetsOutsideRetainedRange(t *testing.T) {
	testTopic := openTestTopic(t, "orders", 1)
	partition, _ := testTopic.Partition(0)
	if _, err := partition.Append([]byte("only")); err != nil {
		t.Fatal(err)
	}
	c, _ := New(partition)
	if _, err := c.Replay("orders", 0, 2, 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Replay() error = %v, want ErrOffsetOutOfRange", err)
	}
}
