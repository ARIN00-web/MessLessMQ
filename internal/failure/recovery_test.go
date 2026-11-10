package failure

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

func TestRecoveryManagerReopensDurableLog(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "orders", "partition-0000000000")

	original, err := storage.OpenLogWithSegmentSize(directory, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.AppendSync([]byte("durable-message")); err != nil {
		t.Fatal(err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}

	manager, err := NewRecoveryManager(1024)
	if err != nil {
		t.Fatal(err)
	}

	recovered, report, err := manager.OpenLog(directory)
	if err != nil {
		t.Fatalf("OpenLog() recovery error: %v", err)
	}
	defer recovered.Close()

	if !report.Verified {
		t.Fatal("recovery report is not verified")
	}
	if report.Records != 1 || report.NextOffset != 1 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}

	got, err := recovered.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "durable-message" {
		t.Fatalf("Read(0) = %q, want durable-message", got)
	}
}

func TestRecoveryManagerRepairsIncompleteActiveTail(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "messages")

	original, err := storage.OpenLog(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.AppendSync([]byte("complete")); err != nil {
		t.Fatal(err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}

	segmentPath := filepath.Join(directory, "00000000000000000000.log")
	file, err := os.OpenFile(segmentPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var partialHeader [storage.RecordHeaderSize]byte
	binary.BigEndian.PutUint32(partialHeader[:storage.RecordLengthSize], 20)
	binary.BigEndian.PutUint64(partialHeader[storage.RecordLengthSize:], 1234)
	if _, err := file.Write(partialHeader[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("cut")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	manager, _ := NewRecoveryManager(storage.DefaultSegmentSize)
	recovered, report, err := manager.OpenLog(directory)
	if err != nil {
		t.Fatalf("OpenLog() after incomplete tail: %v", err)
	}
	defer recovered.Close()

	if report.Records != 1 || report.NextOffset != 1 {
		t.Fatalf("unexpected report after tail repair: %+v", report)
	}
	got, err := recovered.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "complete" {
		t.Fatalf("Read(0) = %q, want complete", got)
	}
}

func TestRecoveryManagerRejectsStructuralCorruption(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "messages")

	original, err := storage.OpenLog(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.AppendSync([]byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}

	segmentPath := filepath.Join(directory, "00000000000000000000.log")
	file, err := os.OpenFile(segmentPath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var badLength [storage.RecordLengthSize]byte
	binary.BigEndian.PutUint32(badLength[:], uint32(storage.MaxRecordSize+1))
	if _, err := file.WriteAt(badLength[:], 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	manager, _ := NewRecoveryManager(storage.DefaultSegmentSize)
	_, _, err = manager.OpenLog(directory)
	if !errors.Is(err, ErrRecoveryFailed) {
		t.Fatalf("OpenLog() error = %v, want ErrRecoveryFailed", err)
	}
	if !errors.Is(err, storage.ErrInvalidRecord) {
		t.Fatalf("OpenLog() error = %v, want underlying ErrInvalidRecord", err)
	}
}

func TestRecoveryManagerRejectsInvalidConfiguration(t *testing.T) {
	if _, err := NewRecoveryManager(0); !errors.Is(err, ErrInvalidRecoveryConfig) {
		t.Fatalf("NewRecoveryManager(0) error = %v", err)
	}

	manager, err := NewRecoveryManager(1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.OpenLog(""); !errors.Is(err, ErrRecoveryFailed) {
		t.Fatalf("OpenLog(empty) error = %v", err)
	}
}

func TestRecoveryManagerOpensEmptyLog(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "empty")
	manager, err := NewRecoveryManager(storage.DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}

	log, report, err := manager.OpenLog(directory)
	if err != nil {
		t.Fatalf("OpenLog() empty directory: %v", err)
	}
	defer log.Close()

	if !report.Verified || report.Records != 0 || report.NextOffset != 0 {
		t.Fatalf("unexpected empty recovery report: %+v", report)
	}
}
