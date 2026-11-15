//go:build windows

package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// LockDir on Windows locks the file using standard exclusive open or LockFileEx.
func LockDir(dir string) (*DirLock, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create lock directory: %w", err)
	}
	lockPath := filepath.Join(dir, ".lock")
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("%w: %s", ErrDirLocked, dir)
		}
		return nil, fmt.Errorf("open lock file %s: %w", lockPath, err)
	}
	return &DirLock{file: file, path: lockPath}, nil
}

// Unlock on Windows removes the lock file and closes it.
func (l *DirLock) Unlock() error {
	if l == nil || l.file == nil {
		return nil
	}
	path := l.path
	err := l.file.Close()
	l.file = nil
	if path != "" {
		_ = os.Remove(path)
	}
	return err
}
