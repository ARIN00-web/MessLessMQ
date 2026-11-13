package replication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCommitStorePersistsAndReloadsHighWatermark(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommitStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.Initialize("orders", 0, 0, 0, "broker-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if initial.HighWatermark != 0 {
		t.Fatalf("initial high watermark = %d, want 0", initial.HighWatermark)
	}
	updated, err := store.Advance("orders", 0, 3, "broker-1", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if updated.HighWatermark != 3 {
		t.Fatalf("updated high watermark = %d, want 3", updated.HighWatermark)
	}

	reopened, err := OpenCommitStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	state, exists := reopened.Get("orders", 0)
	if !exists || state.HighWatermark != 3 || state.LeaderID != "broker-1" || state.LeaderEpoch != 1 {
		t.Fatalf("recovered state = %+v exists=%t", state, exists)
	}
}

func TestCommitStoreRejectsRegressionAheadOfLogAndWrongLeader(t *testing.T) {
	store, err := OpenCommitStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Initialize("orders", 0, 0, 4, "broker-1", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance("orders", 0, 4, "broker-1", 2, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance("orders", 0, 3, "broker-1", 2, 4); !errors.Is(err, ErrCommitRegression) {
		t.Fatalf("regression error = %v, want ErrCommitRegression", err)
	}
	if _, err := store.Advance("orders", 0, 5, "broker-1", 2, 4); !errors.Is(err, ErrCommitAheadOfLog) {
		t.Fatalf("ahead-of-log error = %v, want ErrCommitAheadOfLog", err)
	}
	if _, err := store.Advance("orders", 0, 4, "broker-2", 2, 4); !errors.Is(err, ErrCommitLeader) {
		t.Fatalf("wrong leader error = %v, want ErrCommitLeader", err)
	}
	if _, err := store.Advance("orders", 0, 4, "broker-1", 1, 4); !errors.Is(err, ErrCommitLeader) {
		t.Fatalf("stale epoch error = %v, want ErrCommitLeader", err)
	}
}

func TestCommitStoreInitializeValidatesPersistedIdentityAndRange(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommitStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Initialize("orders", 0, 2, 5, "broker-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Initialize("orders", 0, 1, 5, "broker-1", 1); err != nil {
		t.Fatalf("reinitializing existing state should be idempotent: %v", err)
	}
	if _, err := store.Initialize("orders", 0, 2, 5, "broker-2", 1); !errors.Is(err, ErrCommitLeader) {
		t.Fatalf("identity mismatch error = %v, want ErrCommitLeader", err)
	}
	if _, err := store.Initialize("orders", 0, 2, 1, "broker-1", 1); !errors.Is(err, ErrCommitAheadOfLog) {
		t.Fatalf("ahead-of-log initialization error = %v, want ErrCommitAheadOfLog", err)
	}
}

func TestCommitStoreRejectsCorruptState(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommitStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Initialize("orders", 1, 0, 0, "broker-1", 1); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
	if err := os.WriteFile(filepath.Join(dir, entries[0].Name()), []byte(`{"version":99}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCommitStore(dir); !errors.Is(err, ErrInvalidCommitState) {
		t.Fatalf("corrupt state error = %v, want ErrInvalidCommitState", err)
	}
}

func TestCommitStoreUpdateLeaderPreservesWatermarkAndPersistsEpoch(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommitStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Initialize("orders", 0, 0, 5, "broker-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance("orders", 0, 4, "broker-1", 1, 5); err != nil {
		t.Fatal(err)
	}

	updated, err := store.UpdateLeader("orders", 0, "broker-2", 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	if updated.HighWatermark != 4 || updated.LeaderID != "broker-2" || updated.LeaderEpoch != 2 {
		t.Fatalf("updated state = %+v, want watermark 4, leader broker-2, epoch 2", updated)
	}

	reopened, err := OpenCommitStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	recovered, ok := reopened.Get("orders", 0)
	if !ok || recovered != updated {
		t.Fatalf("recovered state=%+v exists=%t, want %+v", recovered, ok, updated)
	}
}

func TestCommitStoreUpdateLeaderRejectsStaleEpochAndLogBehindWatermark(t *testing.T) {
	store, err := OpenCommitStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Initialize("orders", 0, 0, 5, "broker-1", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance("orders", 0, 4, "broker-1", 2, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateLeader("orders", 0, "broker-2", 1, 5); !errors.Is(err, ErrCommitLeader) {
		t.Fatalf("stale epoch error=%v, want ErrCommitLeader", err)
	}
	if _, err := store.UpdateLeader("orders", 0, "broker-2", 3, 3); !errors.Is(err, ErrCommitAheadOfLog) {
		t.Fatalf("log-behind-watermark error=%v, want ErrCommitAheadOfLog", err)
	}
	state, ok := store.Get("orders", 0)
	if !ok || state.LeaderID != "broker-1" || state.LeaderEpoch != 2 || state.HighWatermark != 4 {
		t.Fatalf("failed update mutated commit state: %+v exists=%t", state, ok)
	}
}
