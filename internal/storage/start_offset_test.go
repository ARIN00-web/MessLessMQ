package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStartOffsetInitiallyZero(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}
	defer log.Close()

	got, err := log.StartOffset()
	if err != nil {
		t.Fatalf("StartOffset() error: %v", err)
	}
	if got != 0 {
		t.Fatalf("StartOffset() = %d, want 0", got)
	}
}

func TestStartOffsetAdvancesAfterRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")
	log, err := OpenLogWithSegmentSize(path, 10)
	if err != nil {
		t.Fatalf("OpenLogWithSegmentSize() error: %v", err)
	}
	defer log.Close()

	now := time.Now().UnixMilli()
	if _, err := log.AppendAt([]byte("expired"), 1_000); err != nil {
		t.Fatalf("AppendAt(expired) error: %v", err)
	}
	if _, err := log.AppendAt([]byte("recent"), now); err != nil {
		t.Fatalf("AppendAt(recent) error: %v", err)
	}

	if err := log.ApplyRetention(RetentionConfig{MaxAge: time.Hour}); err != nil {
		t.Fatalf("ApplyRetention() error: %v", err)
	}

	got, err := log.StartOffset()
	if err != nil {
		t.Fatalf("StartOffset() after retention error: %v", err)
	}
	if got != 1 {
		t.Fatalf("StartOffset() after retention = %d, want 1", got)
	}
}

func TestStartOffsetRejectsClosedLog(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	_, err = log.StartOffset()
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("StartOffset() error = %v, want os.ErrClosed", err)
	}
}

func TestOffsetRangeReturnsConsistentBounds(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}
	defer log.Close()

	if _, err := log.Append([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append([]byte("two")); err != nil {
		t.Fatal(err)
	}

	start, end, err := log.OffsetRange()
	if err != nil {
		t.Fatalf("OffsetRange() error: %v", err)
	}
	if start != 0 || end != 2 {
		t.Fatalf("OffsetRange() = (%d, %d), want (0, 2)", start, end)
	}
}
