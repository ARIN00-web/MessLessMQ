package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func appendRawBytes(t *testing.T, path string, data []byte) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open segment for raw append: %v", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		t.Fatalf("append raw bytes: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close raw segment: %v", err)
	}
}

func TestRecoveryTruncatesIncompleteTrailingHeader(t *testing.T) {
	dir := t.TempDir()

	log, err := OpenLog(dir)
	if err != nil {
		t.Fatal(err)
	}

	offset, err := log.AppendAt([]byte("valid"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Fatalf("first offset = %d, want 0", offset)
	}

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, segmentFilename(0))
	appendRawBytes(t, path, []byte{0x00, 0x00, 0x00})

	recovered, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("OpenLog after partial header: %v", err)
	}
	defer recovered.Close()

	if got := recovered.NextOffset(); got != 1 {
		t.Fatalf("NextOffset() = %d, want 1", got)
	}

	got, err := recovered.Read(0)
	if err != nil {
		t.Fatalf("Read(0): %v", err)
	}
	if string(got) != "valid" {
		t.Fatalf("Read(0) = %q, want %q", got, "valid")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	validRecord, err := EncodeRecordAt([]byte("valid"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(validRecord)) {
		t.Fatalf(
			"segment size = %d, want %d after recovery",
			info.Size(),
			len(validRecord),
		)
	}
}

func TestRecoveryTruncatesIncompleteTrailingPayload(t *testing.T) {
	dir := t.TempDir()

	log, err := OpenLog(dir)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := log.AppendAt([]byte("first"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	partial, err := EncodeRecordAt([]byte("second-message"), 2000)
	if err != nil {
		t.Fatal(err)
	}
	partial = partial[:len(partial)-3]

	path := filepath.Join(dir, segmentFilename(0))
	appendRawBytes(t, path, partial)

	recovered, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("OpenLog after partial payload: %v", err)
	}
	defer recovered.Close()

	if got := recovered.NextOffset(); got != 1 {
		t.Fatalf("NextOffset() = %d, want 1", got)
	}

	first, err := recovered.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != "first" {
		t.Fatalf("Read(0) = %q, want %q", first, "first")
	}

	if _, err := recovered.Read(1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(1) error = %v, want ErrOffsetNotFound", err)
	}

	next, err := recovered.AppendAt([]byte("after-recovery"), 3000)
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if next != 1 {
		t.Fatalf("new offset = %d, want 1", next)
	}

	got, err := recovered.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "after-recovery" {
		t.Fatalf("Read(1) = %q, want %q", got, "after-recovery")
	}
}

func TestRecoveryRejectsImpossiblePayloadLength(t *testing.T) {
	dir := t.TempDir()

	log, err := OpenLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.AppendAt([]byte("valid"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	header := make([]byte, RecordHeaderSize)
	binary.BigEndian.PutUint32(header[:RecordLengthSize], uint32(MaxRecordSize)+1)
	binary.BigEndian.PutUint64(header[RecordLengthSize:], 2000)

	path := filepath.Join(dir, segmentFilename(0))
	appendRawBytes(t, path, header)

	_, err = OpenLog(dir)
	if err == nil {
		t.Fatal("OpenLog() succeeded with an impossible record length; want an error")
	}
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("OpenLog() error = %v, want ErrInvalidRecord", err)
	}
}

func TestRecoveryRebuildsIndexes(t *testing.T) {
	dir := t.TempDir()

	log, err := OpenLog(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if _, err := log.AppendAt([]byte{byte('a' + i)}, int64(1000+i)); err != nil {
			t.Fatal(err)
		}
	}

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	// Destroy both sidecars. The log records remain authoritative.
	if err := os.Remove(filepath.Join(dir, segmentIndexFilename(0))); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, segmentTimeIndexFilename(0))); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("OpenLog should recreate missing indexes: %v", err)
	}
	defer recovered.Close()

	if got := recovered.NextOffset(); got != 3 {
		t.Fatalf("NextOffset() = %d, want 3", got)
	}

	for i, want := range []string{"a", "b", "c"} {
		got, err := recovered.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d): %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("Read(%d) = %q, want %q", i, got, want)
		}
	}

	for _, name := range []string{
		segmentIndexFilename(0),
		segmentTimeIndexFilename(0),
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("expected rebuilt index %s: %v", name, err)
		}
	}
}
