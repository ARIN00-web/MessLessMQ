package group

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/offsets"
	"github.com/ARIN00-web/messlessmq/internal/storage"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func newCoordinatorWithOffsetStore(
	t *testing.T,
	specs map[string]uint32,
) (*Coordinator, *offsets.Store, *topic.Registry, string) {
	t.Helper()

	registry, root := setupRegistry(t, specs)
	store, err := offsets.Open(root)
	if err != nil {
		t.Fatalf("offsets.Open() error: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("offset store Close() error: %v", err)
		}
	})

	coordinator, err := NewCoordinator(registry, store)
	if err != nil {
		t.Fatalf("NewCoordinator() error: %v", err)
	}
	return coordinator, store, registry, root
}

func appendGroupMessages(t *testing.T, registry *topic.Registry, topicName string, partitionID uint32, count int) {
	t.Helper()
	registered, err := registry.Get(topicName)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := registered.Partition(partitionID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if _, err := partition.Append([]byte(fmt.Sprintf("message-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCommitOffsetPersistsNextUnreadPosition(t *testing.T) {
	coordinator, store, registry, _ := newCoordinatorWithOffsetStore(t, map[string]uint32{"orders": 1})
	appendGroupMessages(t, registry, "orders", 0, 4)

	joined, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	fetched, err := coordinator.Fetch("workers", "member-a", joined.Generation, "orders", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.NextOffset != 2 {
		t.Fatalf("Fetch NextOffset = %d, want 2", fetched.NextOffset)
	}

	if err := coordinator.CommitOffset("workers", "member-a", joined.Generation, "orders", 0, fetched.NextOffset); err != nil {
		t.Fatalf("CommitOffset() error: %v", err)
	}

	commit, exists, err := store.Get(offsets.Key{GroupID: "workers", Topic: "orders", PartitionID: 0})
	if err != nil || !exists {
		t.Fatalf("store.Get() error=%v exists=%v", err, exists)
	}
	if commit.NextOffset != 2 {
		t.Fatalf("committed offset = %d, want 2", commit.NextOffset)
	}
}

func TestCommitOffsetRejectsCommitAheadOfReadPosition(t *testing.T) {
	coordinator, _, registry, _ := newCoordinatorWithOffsetStore(t, map[string]uint32{"orders": 1})
	appendGroupMessages(t, registry, "orders", 0, 2)

	joined, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	err = coordinator.CommitOffset("workers", "member-a", joined.Generation, "orders", 0, 1)
	if !errors.Is(err, ErrCommitAheadOfPosition) {
		t.Fatalf("CommitOffset() error = %v, want ErrCommitAheadOfPosition", err)
	}
}

func TestCommitOffsetRejectsOutOfRange(t *testing.T) {
	coordinator, _, registry, _ := newCoordinatorWithOffsetStore(t, map[string]uint32{"orders": 1})
	appendGroupMessages(t, registry, "orders", 0, 1)

	joined, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	err = coordinator.CommitOffset("workers", "member-a", joined.Generation, "orders", 0, 2)
	if !errors.Is(err, ErrCommittedOffsetOutOfRange) {
		t.Fatalf("CommitOffset() error = %v, want ErrCommittedOffsetOutOfRange", err)
	}
}

func TestCommitOffsetRequiresCurrentGenerationAndOwnership(t *testing.T) {
	coordinator, _, _, _ := newCoordinatorWithOffsetStore(t, map[string]uint32{"orders": 4})

	first, err := coordinator.JoinGroup("workers", "a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := coordinator.JoinGroup("workers", "b", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	if err := coordinator.CommitOffset("workers", "a", first.Generation, "orders", 0, 0); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale commit error = %v, want ErrStaleGeneration", err)
	}
	if err := coordinator.CommitOffset("workers", "b", second.Generation, "orders", 0, 0); !errors.Is(err, ErrOffsetCommitNotOwned) {
		t.Fatalf("unowned commit error = %v, want ErrOffsetCommitNotOwned", err)
	}
}

func TestCoordinatorRestoresCommittedOffsetAfterRestart(t *testing.T) {
	registry, root := setupRegistry(t, map[string]uint32{"orders": 1})
	appendGroupMessages(t, registry, "orders", 0, 4)

	store, err := offsets.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(registry, store)
	if err != nil {
		t.Fatal(err)
	}

	joined, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	fetched, err := coordinator.Fetch("workers", "member-a", joined.Generation, "orders", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitOffset("workers", "member-a", joined.Generation, "orders", 0, fetched.NextOffset); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// A new store and coordinator simulate process restart. The topic log
	// remains open, while group membership and consumer objects are rebuilt.
	reopenedStore, err := offsets.Open(root)
	if err != nil {
		t.Fatalf("reopen offset store: %v", err)
	}
	defer reopenedStore.Close()

	restartedCoordinator, err := NewCoordinator(registry, reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	joinedAgain, err := restartedCoordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinGroup() after restart: %v", err)
	}

	next, err := restartedCoordinator.Fetch("workers", "member-a", joinedAgain.Generation, "orders", 0, 1)
	if err != nil {
		t.Fatalf("Fetch() after restart: %v", err)
	}
	if len(next.Records) != 1 || string(next.Records[0].Value) != "message-2" {
		t.Fatalf("first post-restart fetch = %+v, want message-2", next)
	}
}

func TestRebalanceRestoresCommittedOffsetForNewOwner(t *testing.T) {
	coordinator, _, registry, _ := newCoordinatorWithOffsetStore(t, map[string]uint32{"orders": 2})
	appendGroupMessages(t, registry, "orders", 1, 3)

	first, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	fetched, err := coordinator.Fetch("workers", "member-a", first.Generation, "orders", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitOffset("workers", "member-a", first.Generation, "orders", 1, fetched.NextOffset); err != nil {
		t.Fatal(err)
	}

	second, err := coordinator.JoinGroup("workers", "member-b", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	// With two sorted members and two partitions, member-b receives P1.
	if !containsAssignment(second.Assignments, "orders", 1) {
		t.Fatalf("member-b assignments = %+v, want orders/1", second.Assignments)
	}
	resumed, err := coordinator.Fetch("workers", "member-b", second.Generation, "orders", 1, 1)
	if err != nil {
		t.Fatalf("new owner fetch: %v", err)
	}
	if len(resumed.Records) != 1 || string(resumed.Records[0].Value) != "message-1" {
		t.Fatalf("new owner started at wrong offset: %+v", resumed)
	}
}

func TestResetOffsetReplaysPreviouslyReadRecords(t *testing.T) {
	coordinator, store, registry, _ := newCoordinatorWithOffsetStore(t, map[string]uint32{"orders": 1})
	appendGroupMessages(t, registry, "orders", 0, 4)

	joined, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	initial, err := coordinator.Fetch("workers", "member-a", joined.Generation, "orders", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitOffset("workers", "member-a", joined.Generation, "orders", 0, initial.NextOffset); err != nil {
		t.Fatal(err)
	}

	if err := coordinator.ResetOffset("workers", "member-a", joined.Generation, "orders", 0, 1, "reprocess order events"); err != nil {
		t.Fatalf("ResetOffset() error: %v", err)
	}

	commit, exists, err := store.Get(offsets.Key{GroupID: "workers", Topic: "orders", PartitionID: 0})
	if err != nil || !exists {
		t.Fatalf("Get() error=%v exists=%v", err, exists)
	}
	if commit.NextOffset != 1 || !commit.WasReset {
		t.Fatalf("reset commit = %+v, want next offset 1 and WasReset", commit)
	}

	replayed, err := coordinator.Fetch("workers", "member-a", joined.Generation, "orders", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed.Records) != 2 || replayed.Records[0].Offset != 1 || replayed.Records[1].Offset != 2 {
		t.Fatalf("replayed records = %+v, want offsets 1 and 2", replayed.Records)
	}
}

func TestResetCommittedOffsetRecoversAfterRetention(t *testing.T) {
	registry := topic.NewRegistry()
	root := t.TempDir()
	registered, err := registry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := registered.OpenLogs(root, 10); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := registered.CloseLogs(); err != nil {
			t.Errorf("CloseLogs() error: %v", err)
		}
	})
	partition, err := registered.Partition(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append([]byte("first!")); err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append([]byte("second")); err != nil {
		t.Fatal(err)
	}

	store, err := offsets.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key := offsets.Key{GroupID: "workers", Topic: "orders", PartitionID: 0}
	if err := store.Commit(key, 0); err != nil {
		t.Fatal(err)
	}

	if err := partition.ApplyRetention(storage.RetentionConfig{MaxSize: 1}); err != nil {
		t.Fatal(err)
	}

	coordinator, err := NewCoordinator(registry, store)
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if !errors.Is(err, ErrCommittedOffsetOutOfRange) {
		t.Fatalf("JoinGroup() with stale commit error = %v, want ErrCommittedOffsetOutOfRange", err)
	}

	start, _, err := partition.OffsetRange()
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ResetCommittedOffset("workers", "orders", 0, start, "retention advanced start"); err != nil {
		t.Fatalf("ResetCommittedOffset() error: %v", err)
	}

	joined, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinGroup() after reset: %v", err)
	}
	result, err := coordinator.Fetch("workers", "member-a", joined.Generation, "orders", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || string(result.Records[0].Value) != "second" {
		t.Fatalf("fetch after reset = %+v, want retained second message", result)
	}
}

func TestCoordinatorRequiresOffsetStoreForCommit(t *testing.T) {
	registry, _ := setupRegistry(t, map[string]uint32{"orders": 1})
	coordinator, err := NewCoordinator(registry)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	err = coordinator.CommitOffset("workers", "member-a", joined.Generation, "orders", 0, 0)
	if !errors.Is(err, ErrOffsetStoreNotConfigured) {
		t.Fatalf("CommitOffset() error = %v, want ErrOffsetStoreNotConfigured", err)
	}
}

func TestCoordinatorReturnsMissingCommittedOffset(t *testing.T) {
	coordinator, _, _, _ := newCoordinatorWithOffsetStore(t, map[string]uint32{"orders": 1})
	_, exists, err := coordinator.CommittedOffset("workers", "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("unexpected committed offset")
	}
}
