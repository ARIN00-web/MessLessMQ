package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAppendAndRead(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}
	defer log.Close()

	messages := [][]byte{
		[]byte("first message"),
		[]byte("second message"),
		[]byte("third message"),
	}

	for i, message := range messages {
		offset, err := log.Append(message)
		if err != nil {
			t.Fatalf("Append() error: %v", err)
		}
		if offset != uint64(i) {
			t.Fatalf("offset = %d, want %d", offset, i)
		}
	}

	if got := log.NextOffset(); got != uint64(len(messages)) {
		t.Fatalf("NextOffset() = %d, want %d", got, len(messages))
	}

	for i, want := range messages {
		got, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) error: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Read(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestReadUnknownOffset(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}
	defer log.Close()

	_, err = log.Read(0)
	if !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("expected ErrOffsetNotFound, got %v", err)
	}
}

func TestReopenPreservesRecordsAndOffsets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	log, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}

	firstOffset, err := log.Append([]byte("before restart"))
	if err != nil {
		t.Fatalf("Append() error: %v", err)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	log, err = OpenLog(path)
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer log.Close()

	got, err := log.Read(firstOffset)
	if err != nil {
		t.Fatalf("Read() after reopen error: %v", err)
	}
	if string(got) != "before restart" {
		t.Fatalf("Read() = %q, want %q", got, "before restart")
	}

	nextOffset, err := log.Append([]byte("after restart"))
	if err != nil {
		t.Fatalf("Append() after reopen error: %v", err)
	}
	if nextOffset != 1 {
		t.Fatalf("next offset = %d, want 1", nextOffset)
	}
}

func TestConcurrentAppends(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}
	defer log.Close()

	const writers = 20
	const messagesPerWriter = 25
	const total = writers * messagesPerWriter

	var wg sync.WaitGroup
	errs := make(chan error, total)

	for writer := 0; writer < writers; writer++ {
		wg.Add(1)

		go func(writer int) {
			defer wg.Done()

			for message := 0; message < messagesPerWriter; message++ {
				data := fmt.Appendf(nil, "writer-%d-message-%d", writer, message)
				if _, err := log.Append(data); err != nil {
					errs <- err
				}
			}
		}(writer)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent Append() error: %v", err)
	}

	if got := log.NextOffset(); got != total {
		t.Fatalf("NextOffset() = %d, want %d", got, total)
	}

	for offset := uint64(0); offset < total; offset++ {
		if _, err := log.Read(offset); err != nil {
			t.Fatalf("Read(%d) error: %v", offset, err)
		}
	}
}

func TestRejectsIncompleteRecordHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}

	segmentPath := filepath.Join(path, segmentFilename(0))
	if err := os.WriteFile(segmentPath, []byte{0, 0}, 0644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	_, err := OpenLog(path)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

func TestSegmentRollover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	// Each record has a 12-byte header plus its payload.
	log, err := OpenLogWithSegmentSize(path, 10)
	if err != nil {
		t.Fatalf("OpenLogWithSegmentSize() error: %v", err)
	}

	messages := [][]byte{
		[]byte("first!"),
		[]byte("second"),
		[]byte("third!"),
	}

	for i, message := range messages {
		offset, err := log.Append(message)
		if err != nil {
			t.Fatalf("Append() error: %v", err)
		}
		if offset != uint64(i) {
			t.Fatalf("offset = %d, want %d", offset, i)
		}
	}

	if len(log.segments) < 2 {
		t.Fatalf("expected segment rollover, got %d segment(s)", len(log.segments))
	}

	for i, want := range messages {
		got, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) error: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Read(%d) = %q, want %q", i, got, want)
		}
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	log, err = OpenLogWithSegmentSize(path, 10)
	if err != nil {
		t.Fatalf("reopen segmented log: %v", err)
	}
	defer log.Close()

	if got := log.NextOffset(); got != uint64(len(messages)) {
		t.Fatalf("NextOffset() after reopen = %d, want %d", got, len(messages))
	}

	for i, want := range messages {
		got, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) after reopen error: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Read(%d) after reopen = %q, want %q", i, got, want)
		}
	}
}

func TestSegmentFilesUseBaseOffsets(t *testing.T) {
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

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("ReadDir() error: %v", err)
	}

	logFiles := 0
	indexFiles := 0
	timeIndexFiles := 0

	for _, entry := range entries {
		if entry.IsDir() {
			t.Errorf("unexpected directory: %s", entry.Name())
			continue
		}

		switch filepath.Ext(entry.Name()) {
		case ".log":
			logFiles++
		case ".index":
			indexFiles++
		case ".timeindex":
			timeIndexFiles++
		default:
			t.Errorf("unexpected segment entry: %s", entry.Name())
		}
	}

	if logFiles != 3 {
		t.Errorf("log files = %d, want 3", logFiles)
	}

	if indexFiles != 3 {
		t.Errorf("index files = %d, want 3", indexFiles)
	}

	if timeIndexFiles != 3 {
		t.Errorf("time index files = %d, want 3", timeIndexFiles)
	}
}
