package storage

import (
	"errors"
	"os"
)

var ErrDirLocked = errors.New("data directory is locked by another process")

// DirLock represents an advisory exclusive file lock held on a directory.
type DirLock struct {
	file *os.File
	path string
}

// Path returns the path of the lock file.
func (l *DirLock) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}
