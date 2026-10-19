package storage

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// partialWriter deliberately writes only a few bytes per call.
type partialWriter struct {
	buf         bytes.Buffer
	maxPerWrite int
}

func (w *partialWriter) Write(p []byte) (int, error) {
	if len(p) > w.maxPerWrite {
		p = p[:w.maxPerWrite]
	}
	return w.buf.Write(p)
}

// zeroWriter simulates a broken writer that makes no progress.
type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) {
	return 0, nil
}

func TestWriteFullHandlesPartialWrites(t *testing.T) {
	writer := &partialWriter{maxPerWrite: 3}
	want := []byte("MessLessMQ storage I/O")

	if err := writeFull(writer, want); err != nil {
		t.Fatalf("writeFull() error: %v", err)
	}

	if !bytes.Equal(writer.buf.Bytes(), want) {
		t.Fatalf("written bytes = %q, want %q", writer.buf.Bytes(), want)
	}
}

func TestWriteFullRejectsZeroProgress(t *testing.T) {
	err := writeFull(zeroWriter{}, []byte("data"))

	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected io.ErrShortWrite, got %v", err)
	}
}

func TestWriteFullHandlesEmptyInput(t *testing.T) {
	writer := &partialWriter{maxPerWrite: 3}

	if err := writeFull(writer, nil); err != nil {
		t.Fatalf("writeFull() with empty input error: %v", err)
	}

	if writer.buf.Len() != 0 {
		t.Fatalf("wrote %d bytes, want 0", writer.buf.Len())
	}
}
