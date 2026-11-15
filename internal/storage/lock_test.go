package storage

import (
	"errors"
	"testing"
)

func TestLockDirPreventsConcurrentAccess(t *testing.T) {
	dir := t.TempDir()

	lock1, err := LockDir(dir)
	if err != nil {
		t.Fatalf("first LockDir failed: %v", err)
	}
	defer lock1.Unlock()

	// Second lock attempt on same directory must fail with ErrDirLocked.
	lock2, err := LockDir(dir)
	if !errors.Is(err, ErrDirLocked) {
		if lock2 != nil {
			lock2.Unlock()
		}
		t.Fatalf("second LockDir error = %v, want ErrDirLocked", err)
	}

	// Releasing the first lock allows acquiring a new lock.
	if err := lock1.Unlock(); err != nil {
		t.Fatalf("lock1.Unlock failed: %v", err)
	}

	lock3, err := LockDir(dir)
	if err != nil {
		t.Fatalf("third LockDir after unlock failed: %v", err)
	}
	_ = lock3.Unlock()
}
