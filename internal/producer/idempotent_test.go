package producer

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func openProducerTestTopic(t *testing.T, root string, name string, count uint32) (*topic.Topic, []*topic.Partition) {
	t.Helper()
	registry := topic.NewRegistry()
	created, err := registry.CreateWithPartitionCount(name, count)
	if err != nil {
		t.Fatal(err)
	}
	if err := created.OpenLogs(filepath.Join(root, name), 1024); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := created.CloseLogs(); err != nil {
			t.Errorf("CloseLogs() error: %v", err)
		}
	})
	return created, created.Partitions()
}

func TestIdempotentProducerAppendAndDuplicateDetection(t *testing.T) {
	_, partitions := openProducerTestTopic(t, t.TempDir(), "orders", 2)
	manager, err := NewManager(partitions...)
	if err != nil {
		t.Fatal(err)
	}
	session, err := manager.BeginProducer("orders-api")
	if err != nil {
		t.Fatal(err)
	}

	first, err := session.Append(partitions[0], 0, []byte("created"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := session.Append(partitions[0], 1, []byte("paid"))
	if err != nil {
		t.Fatal(err)
	}
	if second.Offset != first.Offset+1 {
		t.Fatalf("second offset = %d, first = %d", second.Offset, first.Offset)
	}

	duplicate, err := session.Append(partitions[0], 1, []byte("paid"))
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.Offset != second.Offset {
		t.Fatalf("duplicate result = %+v", duplicate)
	}

	if _, err := session.Append(partitions[0], 1, []byte("different")); !errors.Is(err, ErrSequenceReuse) {
		t.Fatalf("sequence reuse error = %v", err)
	}
	if _, err := session.Append(partitions[0], 3, []byte("gap")); !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("sequence gap error = %v", err)
	}

	next, err := partitions[0].NextOffset()
	if err != nil {
		t.Fatal(err)
	}
	if next != 2 {
		t.Fatalf("NextOffset = %d, want 2", next)
	}
}

func TestProducerSequencesAreIndependentPerPartition(t *testing.T) {
	_, partitions := openProducerTestTopic(t, t.TempDir(), "orders", 2)
	manager, err := NewManager(partitions...)
	if err != nil {
		t.Fatal(err)
	}
	session, err := manager.BeginProducer("orders-api")
	if err != nil {
		t.Fatal(err)
	}

	for _, partition := range partitions {
		result, err := session.Append(partition, 0, []byte("first"))
		if err != nil {
			t.Fatal(err)
		}
		if result.Offset != 0 {
			t.Fatalf("first offset = %d, want 0", result.Offset)
		}
	}
}

func TestNewProducerEpochFencesOldSession(t *testing.T) {
	_, partitions := openProducerTestTopic(t, t.TempDir(), "orders", 1)
	manager, err := NewManager(partitions...)
	if err != nil {
		t.Fatal(err)
	}
	oldSession, err := manager.BeginProducer("orders-api")
	if err != nil {
		t.Fatal(err)
	}
	newSession, err := manager.BeginProducer("orders-api")
	if err != nil {
		t.Fatal(err)
	}
	if newSession.Epoch() != oldSession.Epoch()+1 {
		t.Fatalf("new epoch = %d, old = %d", newSession.Epoch(), oldSession.Epoch())
	}
	if _, err := oldSession.Append(partitions[0], 0, []byte("stale")); !errors.Is(err, ErrProducerFenced) {
		t.Fatalf("old session append error = %v", err)
	}
	if _, err := newSession.Append(partitions[0], 0, []byte("current")); err != nil {
		t.Fatal(err)
	}
}

func TestIdempotentProducerRecoversSequenceAndDuplicateState(t *testing.T) {
	root := t.TempDir()
	registry := topic.NewRegistry()
	firstTopic, err := registry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(root, "orders")
	if err := firstTopic.OpenLogs(dataDir, 1024); err != nil {
		t.Fatal(err)
	}
	partition, _ := firstTopic.Partition(0)

	manager, err := NewManager(partition)
	if err != nil {
		t.Fatal(err)
	}
	session, err := manager.BeginProducer("orders-api")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(partition, 0, []byte("zero")); err != nil {
		t.Fatal(err)
	}
	last, err := session.Append(partition, 1, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if err := firstTopic.CloseLogs(); err != nil {
		t.Fatal(err)
	}

	secondRegistry := topic.NewRegistry()
	secondTopic, err := secondRegistry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondTopic.OpenLogs(dataDir, 1024); err != nil {
		t.Fatal(err)
	}
	defer secondTopic.CloseLogs()
	secondPartition, _ := secondTopic.Partition(0)

	recoveredManager, err := NewManager(secondPartition)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := recoveredManager.OpenProducer("orders-api", session.Epoch())
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := resumed.Append(secondPartition, 1, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.Offset != last.Offset {
		t.Fatalf("recovered duplicate = %+v", duplicate)
	}
	third, err := resumed.Append(secondPartition, 2, []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if third.Offset != 2 {
		t.Fatalf("third offset = %d, want 2", third.Offset)
	}
}

func TestManagerRejectsUnregisteredPartition(t *testing.T) {
	_, first := openProducerTestTopic(t, t.TempDir(), "orders", 1)
	_, second := openProducerTestTopic(t, t.TempDir(), "payments", 1)
	manager, err := NewManager(first[0])
	if err != nil {
		t.Fatal(err)
	}
	session, err := manager.BeginProducer("producer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(second[0], 0, []byte("x")); !errors.Is(err, ErrPartitionNotRegistered) {
		t.Fatalf("Append() error = %v", err)
	}
}

func TestOpenProducerRejectsOlderEpoch(t *testing.T) {
	_, partitions := openProducerTestTopic(t, t.TempDir(), "orders", 1)
	manager, err := NewManager(partitions...)
	if err != nil {
		t.Fatal(err)
	}
	first, err := manager.BeginProducer("orders-api")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(partitions[0], 0, []byte("one")); err != nil {
		t.Fatal(err)
	}
	second, err := manager.BeginProducer("orders-api")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.OpenProducer("orders-api", first.Epoch()); !errors.Is(err, ErrProducerFenced) {
		t.Fatalf("OpenProducer() with stale epoch error = %v", err)
	}
	if second.Epoch() <= first.Epoch() {
		t.Fatalf("new epoch %d is not greater than old epoch %d", second.Epoch(), first.Epoch())
	}
}
