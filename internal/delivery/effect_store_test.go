package delivery

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestEffectStoreApplyOnceAndRecover(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "effects")
	store, err := OpenEffectStoreWithSegmentSize(directory, 1024)
	if err != nil {
		t.Fatal(err)
	}

	id := EventID{GroupID: "orders-workers", Topic: "orders", PartitionID: 1, Offset: 42}
	effects := [][]byte{[]byte("reserve-inventory"), []byte("publish-order-event")}

	inserted, err := store.ApplyOnce(id, effects)
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("first ApplyOnce() should insert")
	}

	inserted, err = store.ApplyOnce(id, effects)
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("duplicate ApplyOnce() should not insert again")
	}

	if _, err := store.ApplyOnce(id, [][]byte{[]byte("different")}); !errors.Is(err, ErrEffectConflict) {
		t.Fatalf("conflicting event error = %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenEffectStoreWithSegmentSize(directory, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	record, exists, err := recovered.Get(id)
	if err != nil || !exists {
		t.Fatalf("Get() exists=%t err=%v", exists, err)
	}
	if len(record.Effects) != 2 || string(record.Effects[0]) != "reserve-inventory" {
		t.Fatalf("recovered effects = %q", record.Effects)
	}

	inserted, err = recovered.ApplyOnce(id, effects)
	if err != nil || inserted {
		t.Fatalf("recovered duplicate ApplyOnce() = inserted %t err %v", inserted, err)
	}
}

func TestEffectStoreValidatesEventAndEffectLimits(t *testing.T) {
	store, err := OpenEffectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.ApplyOnce(EventID{}, nil); !errors.Is(err, ErrInvalidEventID) {
		t.Fatalf("invalid event error = %v", err)
	}

	tooMany := make([][]byte, maxEffectCount+1)
	if _, err := store.ApplyOnce(EventID{GroupID: "g", Topic: "orders"}, tooMany); !errors.Is(err, ErrInvalidEffects) {
		t.Fatalf("too many effects error = %v", err)
	}
}
