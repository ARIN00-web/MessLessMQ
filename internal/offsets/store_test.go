package offsets

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

func TestOpenUsesReservedInternalTopicDirectory(t *testing.T) {
	dataDir := t.TempDir()
	store, err := Open(dataDir)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer store.Close()

	want := filepath.Join(dataDir, InternalTopicName)
	if store.Directory() != want {
		t.Fatalf("Directory() = %q, want %q", store.Directory(), want)
	}
	info, err := os.Stat(want)
	if err != nil {
		t.Fatalf("Stat(%q): %v", want, err)
	}
	if !info.IsDir() {
		t.Fatalf("%q is not a directory", want)
	}
}

func TestCommitAndGetOffset(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := Key{GroupID: "orders-workers", Topic: "orders", PartitionID: 2}
	if err := store.Commit(key, 17); err != nil {
		t.Fatalf("Commit() error: %v", err)
	}

	got, exists, err := store.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("committed offset not found")
	}
	if got.NextOffset != 17 {
		t.Fatalf("NextOffset = %d, want 17", got.NextOffset)
	}
	if got.Key != key {
		t.Fatalf("Key = %+v, want %+v", got.Key, key)
	}
	if got.CommittedAt.IsZero() {
		t.Fatal("CommittedAt should be populated")
	}

	offset, exists, err := store.GetOffset(key)
	if err != nil || !exists || offset != 17 {
		t.Fatalf("GetOffset() = (%d, %v, %v), want (17, true, nil)", offset, exists, err)
	}
}

func TestCommitIsMonotonicAndResetCanMoveBackward(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := Key{GroupID: "workers", Topic: "orders", PartitionID: 0}
	if err := store.Commit(key, 10); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(key, 15); err != nil {
		t.Fatal(err)
	}

	if err := store.Commit(key, 8); !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("backward Commit() error = %v, want ErrOffsetRegression", err)
	}

	if err := store.Reset(key, 4, "reprocess from event 4"); err != nil {
		t.Fatalf("Reset() error: %v", err)
	}

	got, exists, err := store.Get(key)
	if err != nil || !exists {
		t.Fatalf("Get() = (%v, %v), want existing commit", err, exists)
	}
	if got.NextOffset != 4 || !got.WasReset {
		t.Fatalf("reset commit = %+v, want offset 4 and WasReset", got)
	}

	if err := store.Commit(key, 5); err != nil {
		t.Fatalf("Commit() after reset error: %v", err)
	}
}

func TestCommitWithMetadata(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := Key{GroupID: "analytics", Topic: "orders", PartitionID: 1}
	if err := store.CommitWithMetadata(key, 22, "batch-processed"); err != nil {
		t.Fatal(err)
	}

	got, exists, err := store.Get(key)
	if err != nil || !exists {
		t.Fatalf("Get() error=%v exists=%v", err, exists)
	}
	if got.Metadata != "batch-processed" {
		t.Fatalf("Metadata = %q", got.Metadata)
	}
}

func TestCommitSameValueIsIdempotent(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := Key{GroupID: "workers", Topic: "orders", PartitionID: 0}
	if err := store.Commit(key, 9); err != nil {
		t.Fatal(err)
	}
	before, err := store.LogEndOffset()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(key, 9); err != nil {
		t.Fatal(err)
	}
	after, err := store.LogEndOffset()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("duplicate commit added log record: before=%d after=%d", before, after)
	}
}

func TestStoreSeparatesGroupsAndPartitions(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	entries := []struct {
		key    Key
		offset uint64
	}{
		{Key{GroupID: "group-a", Topic: "orders", PartitionID: 0}, 5},
		{Key{GroupID: "group-a", Topic: "orders", PartitionID: 1}, 7},
		{Key{GroupID: "group-b", Topic: "orders", PartitionID: 0}, 11},
	}
	for _, entry := range entries {
		if err := store.Commit(entry.key, entry.offset); err != nil {
			t.Fatal(err)
		}
	}

	groupA, err := store.ListGroup("group-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(groupA) != 2 {
		t.Fatalf("group-a commit count = %d, want 2", len(groupA))
	}
	if groupA[0].Key.PartitionID != 0 || groupA[1].Key.PartitionID != 1 {
		t.Fatalf("ListGroup() not sorted by partition: %+v", groupA)
	}

	groupB, err := store.ListGroup("group-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(groupB) != 1 || groupB[0].NextOffset != 11 {
		t.Fatalf("unexpected group-b commits: %+v", groupB)
	}
}

func TestStoreRecoversLatestCommitAfterReopen(t *testing.T) {
	dataDir := t.TempDir()
	key := Key{GroupID: "workers", Topic: "orders", PartitionID: 2}

	store, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(key, 4); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(key, 8); err != nil {
		t.Fatal(err)
	}
	if err := store.Reset(key, 3, "operator replay"); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(key, 6); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	got, exists, err := reopened.Get(key)
	if err != nil || !exists {
		t.Fatalf("Get() error=%v exists=%v", err, exists)
	}
	if got.NextOffset != 6 || got.WasReset {
		t.Fatalf("recovered commit = %+v, want final offset 6", got)
	}
}

func TestStoreRejectsCorruptJournalRecord(t *testing.T) {
	dataDir := t.TempDir()
	store, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	journal, err := storage.OpenLog(filepath.Join(dataDir, InternalTopicName))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Append([]byte("not-an-offset-record")); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dataDir)
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Open() error = %v, want ErrCorruptRecord", err)
	}
}

func TestStoreRejectsInvalidKeysAndMetadata(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	tests := []struct {
		name string
		key  Key
	}{
		{"empty group", Key{Topic: "orders"}},
		{"invalid group", Key{GroupID: "bad/group", Topic: "orders"}},
		{"empty topic", Key{GroupID: "workers"}},
		{"invalid topic", Key{GroupID: "workers", Topic: "orders/payments"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := store.Commit(tt.key, 1); !errors.Is(err, ErrInvalidKey) {
				t.Fatalf("Commit() error = %v, want ErrInvalidKey", err)
			}
		})
	}

	key := Key{GroupID: "workers", Topic: "orders"}
	if err := store.CommitWithMetadata(key, 1, string(make([]byte, maxMetadataLength+1))); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatalf("metadata error = %v, want ErrInvalidMetadata", err)
	}
}

func TestStoreCloseIsIdempotentAndRejectsOperations(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	key := Key{GroupID: "workers", Topic: "orders"}
	if err := store.Commit(key, 1); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Commit() error = %v, want ErrStoreClosed", err)
	}
	if _, _, err := store.Get(key); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Get() error = %v, want ErrStoreClosed", err)
	}
}

func TestStoreRejectsInvalidOpenConfiguration(t *testing.T) {
	if _, err := Open(""); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Open empty path error = %v", err)
	}
	if _, err := OpenWithSegmentSize(t.TempDir(), 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("OpenWithSegmentSize zero error = %v", err)
	}
}
