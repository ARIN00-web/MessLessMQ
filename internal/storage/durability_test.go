package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendSyncAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	log, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}

	offset, err := log.AppendSync([]byte("durable message"))
	if err != nil {
		t.Fatalf("AppendSync() error: %v", err)
	}

	if offset != 0 {
		t.Fatalf("offset = %d, want 0", offset)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	log, err = OpenLog(path)
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer log.Close()

	got, err := log.Read(offset)
	if err != nil {
		t.Fatalf("Read() after reopen error: %v", err)
	}

	want := []byte("durable message")
	if !bytes.Equal(got, want) {
		t.Fatalf("Read() = %q, want %q", got, want)
	}
}

func TestSyncAfterAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")

	log, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}
	defer log.Close()

	if _, err := log.Append([]byte("first")); err != nil {
		t.Fatalf("Append() error: %v", err)
	}
	if _, err := log.Append([]byte("second")); err != nil {
		t.Fatalf("Append() error: %v", err)
	}

	if err := log.Sync(); err != nil {
		t.Fatalf("Sync() error: %v", err)
	}
}

func TestSyncClosedLog(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatalf("OpenLog() error: %v", err)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	if err := log.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed, got %v", err)
	}

	if _, err := log.AppendSync([]byte("after close")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed from AppendSync(), got %v", err)
	}
}
