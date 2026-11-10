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

func setupDeliveryGroup(t *testing.T, messageCount int) (*group.Coordinator, *offsets.Store, *topic.Partition, group.JoinResult) {
	t.Helper()
	registry := topic.NewRegistry()
	orders, err := registry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := orders.OpenLogs(filepath.Join(t.TempDir(), "data"), 1024); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := orders.CloseLogs(); err != nil {
			t.Errorf("CloseLogs() error: %v", err)
		}
	})
	partition, err := orders.Partition(0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < messageCount; i++ {
		if _, err := partition.Append([]byte(fmt.Sprintf("event-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	store, err := offsets.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("offset store Close() error: %v", err)
		}
	})
	coordinator, err := group.NewCoordinator(registry, store)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := coordinator.JoinGroup("workers", "worker-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, store, partition, joined
}

func TestAtMostOnceCommitsBeforeProcessing(t *testing.T) {
	coordinator, _, _, joined := setupDeliveryGroup(t, 2)
	processingFailure := errors.New("application failed")
	processor, err := NewProcessor(coordinator, AtMostOnce, func(context.Context, consumer.Record) error {
		return processingFailure
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := processor.ProcessBatch(
		context.Background(), "workers", joined.MemberID, joined.Generation,
		"orders", 0, 2,
	)
	if err == nil || !errors.Is(err, processingFailure) {
		t.Fatalf("ProcessBatch() error = %v, want processing failure", err)
	}
	if result.CommittedNextOffset != 2 {
		t.Fatalf("committed offset = %d, want 2", result.CommittedNextOffset)
	}
	if result.Processed != 0 {
		t.Fatalf("processed = %d, want 0", result.Processed)
	}
	commit, exists, err := coordinator.CommittedOffset("workers", "orders", 0)
	if err != nil || !exists || commit.NextOffset != 2 {
		t.Fatalf("CommittedOffset() = %+v, exists=%t, err=%v", commit, exists, err)
	}
}

func TestAtLeastOnceCommitsAfterEachSuccessfulRecordAndReplaysFailure(t *testing.T) {
	coordinator, _, _, joined := setupDeliveryGroup(t, 3)
	calls := make(map[uint64]int)
	failure := errors.New("uncertain processing outcome")
	processor, err := NewProcessor(coordinator, AtLeastOnce, func(_ context.Context, record consumer.Record) error {
		calls[record.Offset]++
		if record.Offset == 1 && calls[record.Offset] == 1 {
			// The application may have performed its side effect before
			// reporting failure. At-least-once can therefore redeliver it.
			return failure
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := processor.ProcessBatch(
		context.Background(), "workers", joined.MemberID, joined.Generation,
		"orders", 0, 3,
	)
	if err == nil || !errors.Is(err, failure) {
		t.Fatalf("first ProcessBatch() error = %v", err)
	}
	if first.CommittedNextOffset != 1 {
		t.Fatalf("first committed offset = %d, want 1", first.CommittedNextOffset)
	}

	// Use a processor on the same group. Offset 1 is delivered again, which
	// demonstrates the at-least-once duplicate window.
	second, err := processor.ProcessBatch(
		context.Background(), "workers", joined.MemberID, joined.Generation,
		"orders", 0, 3,
	)
	if err != nil {
		t.Fatalf("retry ProcessBatch() error: %v", err)
	}
	if second.Processed != 2 {
		t.Fatalf("retry processed = %d, want 2", second.Processed)
	}
	if calls[0] != 1 || calls[1] != 2 || calls[2] != 1 {
		t.Fatalf("handler calls by offset = %v, want 0:1 1:2 2:1", calls)
	}
}

func TestProcessorRejectsInvalidConfiguration(t *testing.T) {
	if _, err := NewProcessor(nil, AtMostOnce, func(context.Context, consumer.Record) error { return nil }); !errors.Is(err, ErrNilCoordinator) {
		t.Fatalf("nil coordinator error = %v", err)
	}
	coordinator, _, _, _ := setupDeliveryGroup(t, 0)
	if _, err := NewProcessor(coordinator, Mode(99), func(context.Context, consumer.Record) error { return nil }); !errors.Is(err, ErrInvalidMode) {
		t.Fatalf("invalid mode error = %v", err)
	}
	if _, err := NewProcessor(coordinator, AtLeastOnce, nil); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("nil handler error = %v", err)
	}
}

func TestAtMostOnceCommitFailureDoesNotInvokeHandler(t *testing.T) {
	coordinator, store, _, joined := setupDeliveryGroup(t, 1)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	called := false
	processor, err := NewProcessor(coordinator, AtMostOnce, func(context.Context, consumer.Record) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = processor.ProcessBatch(context.Background(), "workers", joined.MemberID, joined.Generation, "orders", 0, 10)
	if err == nil {
		t.Fatal("expected commit failure")
	}
	if called {
		t.Fatal("handler ran before the at-most-once commit succeeded")
	}
	batch, fetchErr := coordinator.Fetch("workers", joined.MemberID, joined.Generation, "orders", 0, 1)
	if fetchErr != nil {
		t.Fatal(fetchErr)
	}
	if len(batch.Records) != 1 || batch.Records[0].Offset != 0 {
		t.Fatalf("read position was not rewound after commit failure: %+v", batch)
	}
}
