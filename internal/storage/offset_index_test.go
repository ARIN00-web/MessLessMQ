package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestSparseIndexCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	log, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < int(OffsetIndexInterval)+2; i++ {
		if _, err := log.Append([]byte("message")); err != nil {
			t.Fatal(err)
		}
	}

	if len(log.segments[0].indexEntries) != 2 {
		t.Fatalf("index entries = %d, want 2",
			len(log.segments[0].indexEntries))
	}

	info, err := os.Stat(filepath.Join(path, segmentIndexFilename(0)))
	if err != nil {
		t.Fatal(err)
	}

	wantSize := int64(2 * OffsetIndexEntrySize)
	if info.Size() != wantSize {
		t.Fatalf("index size = %d, want %d", info.Size(), wantSize)
	}

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadUsesSparseIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	log, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	messages := make([][]byte, 300)
	for i := range messages {
		messages[i] = []byte{byte(i >> 8), byte(i), 'x'}
		if _, err := log.Append(messages[i]); err != nil {
			t.Fatal(err)
		}
	}

	for _, offset := range []uint64{0, 1, 127, 128, 129, 255, 299} {
		got, err := log.Read(offset)
		if err != nil {
			t.Fatalf("Read(%d): %v", offset, err)
		}
		if !bytes.Equal(got, messages[offset]) {
			t.Errorf("Read(%d) = %v, want %v",
				offset, got, messages[offset])
		}
	}
}

func TestIndexRebuiltOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	log, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 260; i++ {
		if _, err := log.Append([]byte("persistent message")); err != nil {
			t.Fatal(err)
		}
	}

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the index. The log records remain intact.
	indexPath := filepath.Join(path, segmentIndexFilename(0))
	if err := os.WriteFile(indexPath, []byte{1, 2, 3}, 0644); err != nil {
		t.Fatal(err)
	}

	log, err = OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog() should rebuild the index: %v", err)
	}
	defer log.Close()

	if len(log.segments[0].indexEntries) != 3 {
		t.Fatalf("rebuilt entries = %d, want 3",
			len(log.segments[0].indexEntries))
	}

	info, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() != 3*OffsetIndexEntrySize {
		t.Fatalf("rebuilt index size = %d, want %d",
			info.Size(), 3*OffsetIndexEntrySize)
	}

	got, err := log.Read(259)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("persistent message")) {
		t.Fatalf("unexpected payload: %q", got)
	}
}

func TestIndexEntryEncoding(t *testing.T) {
	entry := indexEntry{offset: 42, position: 1234}
	encoded := encodeIndexEntry(entry)

	if len(encoded) != OffsetIndexEntrySize {
		t.Fatalf("encoded size = %d, want %d",
			len(encoded), OffsetIndexEntrySize)
	}

	if got := binary.BigEndian.Uint64(encoded[:8]); got != 42 {
		t.Fatalf("encoded offset = %d, want 42", got)
	}

	if got := binary.BigEndian.Uint64(encoded[8:]); got != 1234 {
		t.Fatalf("encoded position = %d, want 1234", got)
	}
}
