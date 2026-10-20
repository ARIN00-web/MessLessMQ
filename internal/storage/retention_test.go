
package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTimeRetentionDeletesExpiredSegmentsAndPreservesOffsets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	log, err := OpenLogWithSegmentSize(path, 10)
	if err != nil {
		t.Fatalf("OpenLogWithSegmentSize() error: %v", err)
	}

	now := time.Now().UnixMilli()

	if offset, err := log.AppendAt([]byte("expired"), 1_000); err != nil || offset != 0 {
		t.Fatalf("AppendAt(expired) = (%d, %v), want (0, nil)", offset, err)
	}
	if offset, err := log.AppendAt([]byte("recent-1"), now); err != nil || offset != 1 {
		t.Fatalf("AppendAt(recent-1) = (%d, %v), want (1, nil)", offset, err)
	}
	if offset, err := log.AppendAt([]byte("recent-2"), now); err != nil || offset != 2 {
		t.Fatalf("AppendAt(recent-2) = (%d, %v), want (2, nil)", offset, err)
	}

	if err := log.ApplyRetention(RetentionConfig{
		MaxAge: time.Hour,
	}); err != nil {
		t.Fatalf("ApplyRetention() error: %v", err)
	}

	if len(log.segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(log.segments))
	}
	if log.segments[0].baseOffset != 1 {
		t.Fatalf("first surviving base offset = %d, want 1", log.segments[0].baseOffset)
	}

	if _, err := log.Read(0); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(0) error = %v, want ErrOffsetNotFound", err)
	}

	got, err := log.Read(1)
	if err != nil {
		t.Fatalf("Read(1) error: %v", err)
	}
	if string(got) != "recent-1" {
		t.Fatalf("Read(1) = %q, want %q", got, "recent-1")
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	log, err = OpenLogWithSegmentSize(path, 10)
	if err != nil {
		t.Fatalf("reopen after retention: %v", err)
	}
	defer log.Close()

	if log.NextOffset() != 3 {
		t.Fatalf("NextOffset() after reopen = %d, want 3", log.NextOffset())
	}

	got, err = log.Read(2)
	if err != nil {
		t.Fatalf("Read(2) after reopen error: %v", err)
	}
	if string(got) != "recent-2" {
		t.Fatalf("Read(2) after reopen = %q, want %q", got, "recent-2")
	}
}

func TestSizeRetentionDeletesOldestSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	log, err := OpenLogWithSegmentSize(path, 10)
	if err != nil {
		t.Fatalf("OpenLogWithSegmentSize() error: %v", err)
	}

	for _, message := range [][]byte{
		[]byte("first!"),
		[]byte("second"),
		[]byte("third!"),
	} {
		if _, err := log.Append(message); err != nil {
			t.Fatalf("Append() error: %v", err)
		}
	}

	before, err := totalStorageSize(path)
	if err != nil {
		t.Fatalf("totalStorageSize() before retention: %v", err)
	}

	if err := log.ApplyRetention(RetentionConfig{
		MaxSize: 120,
	}); err != nil {
		t.Fatalf("ApplyRetention() error: %v", err)
	}

	after, err := totalStorageSize(path)
	if err != nil {
		t.Fatalf("totalStorageSize() after retention: %v", err)
	}

	if after >= before {
		t.Fatalf("storage size after retention = %d, want less than %d", after, before)
	}
	if after > 120 {
		t.Fatalf("storage size after retention = %d, want <= 120", after)
	}

	if len(log.segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(log.segments))
	}

	if log.segments[len(log.segments)-1].baseOffset != 2 {
		t.Fatalf("active segment base offset = %d, want 2",
			log.segments[len(log.segments)-1].baseOffset)
	}

	if _, err := log.Read(0); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(0) error = %v, want ErrOffsetNotFound", err)
	}

	got, err := log.Read(2)
	if err != nil {
		t.Fatalf("Read(2) error: %v", err)
	}
	if string(got) != "third!" {
		t.Fatalf("Read(2) = %q, want %q", got, "third!")
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	log, err = OpenLogWithSegmentSize(path, 10)
	if err != nil {
		t.Fatalf("reopen after size retention: %v", err)
	}
	defer log.Close()

	if log.NextOffset() != 3 {
		t.Fatalf("NextOffset() after reopen = %d, want 3", log.NextOffset())
	}
}

func TestRetentionNeverDeletesOnlyActiveSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	log, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}
	defer log.Close()

	if _, err := log.Append([]byte("keep me")); err != nil {
		t.Fatalf("Append() error: %v", err)
	}

	if err := log.ApplyRetention(RetentionConfig{
		MaxAge:  time.Nanosecond,
		MaxSize: 1,
	}); err != nil {
		t.Fatalf("ApplyRetention() error: %v", err)
	}

	if len(log.segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(log.segments))
	}

	got, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read(0) error: %v", err)
	}
	if string(got) != "keep me" {
		t.Fatalf("Read(0) = %q, want %q", got, "keep me")
	}
}

func TestRetentionRejectsNegativeConfiguration(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}
	defer log.Close()

	tests := []struct {
		name string
		cfg  RetentionConfig
	}{
		{
			name: "negative age",
			cfg:  RetentionConfig{MaxAge: -time.Second},
		},
		{
			name: "negative size",
			cfg:  RetentionConfig{MaxSize: -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := log.ApplyRetention(tt.cfg); err == nil {
				t.Fatal("ApplyRetention() expected an error")
			}
		})
	}
}

func TestRetentionRejectsClosedLog(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	err = log.ApplyRetention(RetentionConfig{MaxSize: 1})
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("ApplyRetention() error = %v, want os.ErrClosed", err)
	}
}

func totalStorageSize(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}

	return total, nil
}
