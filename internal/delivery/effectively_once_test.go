package delivery

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/consumer"
	"github.com/ARIN00-web/messlessmq/internal/group"
	"github.com/ARIN00-web/messlessmq/internal/offsets"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func TestProcessEffectivelyOnceDeduplicatesEffectsAfterRetry(t *testing.T) {
	registry := topic.NewRegistry()
	orders, err := registry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := orders.OpenLogs(dataDir, 1024); err != nil {
		t.Fatal(err)
	}
	defer orders.CloseLogs()
	partition, _ := orders.Partition(0)
	for i := 0; i < 2; i++ {
		if _, err := partition.Append([]byte(fmt.Sprintf("event-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	offsetStore, err := offsets.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer offsetStore.Close()
	coordinator, err := group.NewCoordinator(registry, offsetStore)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := coordinator.JoinGroup("workers", "worker-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	effectDir := filepath.Join(t.TempDir(), "effects")
	effectStore, err := OpenEffectStoreWithSegmentSize(effectDir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer effectStore.Close()

	processor, err := NewProcessor(coordinator, AtLeastOnce, func(context.Context, consumer.Record) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	calls := map[uint64]int{}
	failure := errors.New("temporary handler failure")
	handler := func(_ context.Context, record consumer.Record) ([][]byte, error) {
		calls[record.Offset]++
		if record.Offset == 1 && calls[record.Offset] == 1 {
			return nil, failure
		}
		return [][]byte{[]byte("effect-for-" + string(record.Value))}, nil
	}

	_, err = processor.ProcessEffectivelyOnce(
		context.Background(), effectStore, "workers", joined.MemberID,
		joined.Generation, "orders", 0, 10, handler,
	)
	if err == nil || !errors.Is(err, failure) {
		t.Fatalf("first ProcessEffectivelyOnce() error = %v", err)
	}

	// The first input's effect was persisted before the second input failed.
	firstID := EventID{GroupID: "workers", Topic: "orders", PartitionID: 0, Offset: 0}
	if _, exists, err := effectStore.Get(firstID); err != nil || !exists {
		t.Fatalf("first effect exists=%t err=%v", exists, err)
	}

	result, err := processor.ProcessEffectivelyOnce(
		context.Background(), effectStore, "workers", joined.MemberID,
		joined.Generation, "orders", 0, 10, handler,
	)
	if err != nil {
		t.Fatalf("retry ProcessEffectivelyOnce() error: %v", err)
	}
	if result.CommittedNextOffset != 2 {
		t.Fatalf("committed offset = %d, want 2", result.CommittedNextOffset)
	}
	if calls[0] != 1 {
		t.Fatalf("handler calls for first event = %d, want 1 (deduplicated)", calls[0])
	}
	if calls[1] != 2 {
		t.Fatalf("handler calls for second event = %d, want 2", calls[1])
	}
}
