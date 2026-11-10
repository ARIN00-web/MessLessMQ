package failure

import (
	"errors"
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

var (
	ErrRecoveryFailed        = errors.New("storage recovery failed")
	ErrInvalidRecoveryConfig = errors.New("invalid recovery manager configuration")
)

// RecoveryManager opens a log using the storage engine's existing startup
// recovery, then verifies the recovered log and its rebuildable indexes.
type RecoveryManager struct {
	maxSegmentBytes int64
}

// RecoveryReport summarizes the verified log state after startup recovery.
type RecoveryReport struct {
	Directory   string
	StartOffset uint64
	NextOffset  uint64
	Segments    int
	Records     uint64
	DataBytes   int64
	Verified    bool
}

// NewRecoveryManager creates a recovery manager with a positive segment size.
func NewRecoveryManager(maxSegmentBytes int64) (*RecoveryManager, error) {
	if maxSegmentBytes <= 0 {
		return nil, ErrInvalidRecoveryConfig
	}
	return &RecoveryManager{maxSegmentBytes: maxSegmentBytes}, nil
}

// OpenLog opens an existing or new log. OpenLog performs its normal startup
// behavior: repair an incomplete tail in the active segment and rebuild
// sidecar indexes. This method then runs structural verification before
// returning the log to the caller.
//
// On success, ownership of the returned log belongs to the caller, who must
// close it. On verification failure, the method closes the log before return.
func (m *RecoveryManager) OpenLog(directory string) (*storage.Log, RecoveryReport, error) {
	if m == nil || m.maxSegmentBytes <= 0 || directory == "" {
		return nil, RecoveryReport{}, ErrRecoveryFailed
	}

	log, err := storage.OpenLogWithSegmentSize(directory, m.maxSegmentBytes)
	if err != nil {
		return nil, RecoveryReport{}, fmt.Errorf("%w: open %s: %w", ErrRecoveryFailed, directory, err)
	}

	verification, err := log.Verify()
	if err != nil {
		closeErr := log.Close()
		return nil, RecoveryReport{}, errors.Join(
			fmt.Errorf("%w: verify %s: %w", ErrRecoveryFailed, directory, err),
			closeErr,
		)
	}

	start, next, err := log.OffsetRange()
	if err != nil {
		closeErr := log.Close()
		return nil, RecoveryReport{}, errors.Join(
			fmt.Errorf("%w: read recovered offset range: %w", ErrRecoveryFailed, err),
			closeErr,
		)
	}

	return log, RecoveryReport{
		Directory:   directory,
		StartOffset: start,
		NextOffset:  next,
		Segments:    verification.Segments,
		Records:     verification.Records,
		DataBytes:   verification.Bytes,
		Verified:    true,
	}, nil
}
