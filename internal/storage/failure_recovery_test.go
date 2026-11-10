package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

var errInjectedPartialWrite = errors.New("injected failure after partial write")

type partialThenFailWriter struct {
	writer    io.Writer
	remaining int
}

func (w *partialThenFailWriter) Write(data []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errInjectedPartialWrite
	}
	if len(data) > w.remaining {
		data = data[:w.remaining]
	}
	n, err := w.writer.Write(data)
	w.remaining -= n
	return n, err
}

// A partial record write is an incomplete tail. After a process crash, startup
// recovery truncates that tail and retains earlier complete records.
func TestPartialWriteAtActiveTailIsRecoveredAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")
	log, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := log.AppendSync([]byte("durable-before-crash")); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	segmentPath := filepath.Join(path, segmentFilename(0))
	file, err := os.OpenFile(segmentPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}

	partialRecord, err := EncodeRecord([]byte("incomplete-record"))
	if err != nil {
		t.Fatal(err)
	}
	writer := &partialThenFailWriter{
		writer:    file,
		remaining: RecordHeaderSize + 3,
	}

	writeErr := writeFull(writer, partialRecord)
	if !errors.Is(writeErr, errInjectedPartialWrite) {
		t.Fatalf("writeFull() error = %v, want injected partial-write error", writeErr)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog() after partial write: %v", err)
	}
	defer reopened.Close()

	if reopened.NextOffset() != 1 {
		t.Fatalf("NextOffset() = %d, want 1", reopened.NextOffset())
	}

	got, err := reopened.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "durable-before-crash" {
		t.Fatalf("Read(0) = %q, want durable-before-crash", got)
	}

	_, err = reopened.Read(1)
	if !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(1) error = %v, want ErrOffsetNotFound", err)
	}
}
