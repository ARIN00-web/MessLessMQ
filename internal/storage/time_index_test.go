package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadWithTimestamp(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	offset, err := log.AppendAt([]byte("event"), 123456)
	if err != nil {
		t.Fatal(err)
	}

	payload, timestamp, err := log.ReadWithTimestamp(offset)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "event" || timestamp != 123456 {
		t.Fatalf("got (%q, %d), want (%q, %d)", payload, timestamp, "event", 123456)
	}
}

func TestReadAtOrAfterTimestamp(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	records := []struct {
		payload string
		ts      int64
	}{
		{"first", 100},
		{"second", 300},
		{"third", 200},
		{"fourth", 500},
	}

	for _, r := range records {
		if _, err := log.AppendAt([]byte(r.payload), r.ts); err != nil {
			t.Fatal(err)
		}
	}

	offset, payload, ts, err := log.ReadAtOrAfterTimestamp(250)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 1 || string(payload) != "second" || ts != 300 {
		t.Fatalf("got offset=%d payload=%q timestamp=%d", offset, payload, ts)
	}
}

func TestTimeIndexRebuiltOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages")
	log, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 300; i++ {
		if _, err := log.AppendAt([]byte("event"), int64(i*10)); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	timeIndexPath := filepath.Join(path, segmentTimeIndexFilename(0))
	if err := os.WriteFile(timeIndexPath, []byte{1, 2, 3}, 0644); err != nil {
		t.Fatal(err)
	}

	log, err = OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	info, err := os.Stat(timeIndexPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 3*TimeIndexEntrySize {
		t.Fatalf("time index size = %d, want %d", info.Size(), 3*TimeIndexEntrySize)
	}

	offset, payload, ts, err := log.ReadAtOrAfterTimestamp(2570)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 257 || string(payload) != "event" || ts != 2570 {
		t.Fatalf("got offset=%d payload=%q timestamp=%d", offset, payload, ts)
	}
}

func TestReadAtOrAfterTimestampNotFound(t *testing.T) {
	log, err := OpenLog(filepath.Join(t.TempDir(), "messages"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	if _, err := log.AppendAt([]byte("old"), 10); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = log.ReadAtOrAfterTimestamp(20)
	if !errors.Is(err, ErrTimestampNotFound) {
		t.Fatalf("expected ErrTimestampNotFound, got %v", err)
	}
}
