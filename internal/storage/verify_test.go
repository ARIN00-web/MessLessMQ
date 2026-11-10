package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyReportsHealthySegmentedLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")
	log, err := OpenLogWithSegmentSize(path, 20)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	messages := []string{"first", "second", "third", "fourth"}
	for _, message := range messages {
		if _, err := log.Append([]byte(message)); err != nil {
			t.Fatal(err)
		}
	}

	report, err := log.Verify()
	if err != nil {
		t.Fatalf("Verify() error: %v", err)
	}
	if report.Segments < 2 {
		t.Fatalf("Segments = %d, want at least 2", report.Segments)
	}
	if report.Records != uint64(len(messages)) {
		t.Fatalf("Records = %d, want %d", report.Records, len(messages))
	}
	if report.NextOffset != uint64(len(messages)) {
		t.Fatalf("NextOffset = %d, want %d", report.NextOffset, len(messages))
	}
}

func TestVerifyDetectsCorruptRecordHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")
	log, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	if _, err := log.Append([]byte("valid")); err != nil {
		t.Fatal(err)
	}

	var invalidLength [RecordLengthSize]byte
	binary.BigEndian.PutUint32(invalidLength[:], uint32(MaxRecordSize+1))
	if _, err := log.segments[0].file.WriteAt(invalidLength[:], 0); err != nil {
		t.Fatal(err)
	}

	_, err = log.Verify()
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("Verify() error = %v, want ErrInvalidRecord", err)
	}
}

func TestVerifyDetectsCorruptIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")
	log, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	if _, err := log.Append([]byte("indexed")); err != nil {
		t.Fatal(err)
	}

	if _, err := log.segments[0].indexFile.WriteAt([]byte{0xFF}, 0); err != nil {
		t.Fatal(err)
	}

	_, err = log.Verify()
	if !errors.Is(err, ErrCorruptIndex) {
		t.Fatalf("Verify() error = %v, want ErrCorruptIndex", err)
	}
}

func TestVerifyRejectsClosedLog(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = log.Verify()
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Verify() error = %v, want os.ErrClosed", err)
	}
}
