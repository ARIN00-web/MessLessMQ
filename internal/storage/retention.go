package storage

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// RetentionConfig controls when old log segments can be removed.
//
// A zero value disables that retention policy:
//   - MaxAge == 0 disables time-based retention.
//   - MaxSize == 0 disables size-based retention.
//
// The active segment is never deleted, even if it exceeds MaxSize.
type RetentionConfig struct {
	MaxAge  time.Duration
	MaxSize int64
}

// ApplyRetention removes eligible closed segments from the beginning of the log.
//
// Time retention deletes a segment only when its maximum record timestamp is
// strictly earlier than the cutoff. This conservative rule handles records
// whose timestamps are out of order.
//
// Size retention counts each segment's .log, .index, and .timeindex files.
// It removes the oldest segments until the configured limit is met or only
// the active segment remains.
//
// Retention is explicit: this method does not start a background scheduler.
//
// The method holds the log's exclusive lock for the operation so appends,
// reads, and other retention operations cannot observe a partially updated
// in-memory segment list.
func (l *Log) ApplyRetention(cfg RetentionConfig) error {
	if cfg.MaxAge < 0 {
		return fmt.Errorf("max age cannot be negative")
	}
	if cfg.MaxSize < 0 {
		return fmt.Errorf("max size cannot be negative")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return os.ErrClosed
	}

	if cfg.MaxAge == 0 && cfg.MaxSize == 0 {
		return nil
	}

	// Keep at least one segment. The final segment is the active segment.
	if len(l.segments) <= 1 {
		return nil
	}

	closedCount := len(l.segments) - 1
	deleteCount := 0

	// First determine the prefix eligible for time-based retention.
	if cfg.MaxAge > 0 {
		cutoff := time.Now().Add(-cfg.MaxAge).UnixMilli()

		for deleteCount < closedCount {
			seg := l.segments[deleteCount]

			maxTimestamp, found, err := segmentMaxTimestamp(seg)
			if err != nil {
				return fmt.Errorf(
					"inspect timestamps in segment %d: %w",
					seg.baseOffset,
					err,
				)
			}

			// Empty segments are retained conservatively.
			if !found || maxTimestamp >= cutoff {
				break
			}

			deleteCount++
		}
	}

	// Size retention may extend the prefix already selected by time retention.
	if cfg.MaxSize > 0 {
		sizes := make([]int64, len(l.segments))
		var totalSize int64

		for i, seg := range l.segments {
			size, err := segmentStorageSize(seg)
			if err != nil {
				return fmt.Errorf(
					"calculate storage size for segment %d: %w",
					seg.baseOffset,
					err,
				)
			}

			sizes[i] = size
			totalSize += size
		}

		// Time-selected segments are already scheduled for deletion.
		for i := 0; i < deleteCount; i++ {
			totalSize -= sizes[i]
		}

		// Never delete the active segment.
		for totalSize > cfg.MaxSize && deleteCount < closedCount {
			totalSize -= sizes[deleteCount]
			deleteCount++
		}
	}

	if deleteCount == 0 {
		return nil
	}

	// Delete files before changing the in-memory segment list. If deletion
	// fails, leave the list intact so the failure is visible to the caller.
	for i := 0; i < deleteCount; i++ {
		if err := deleteSegmentFiles(l.segments[i]); err != nil {
			return fmt.Errorf(
				"delete segment %d: %w",
				l.segments[i].baseOffset,
				err,
			)
		}
	}

	// Remove the deleted prefix only after all selected segments were deleted.
	l.segments = l.segments[deleteCount:]

	return nil
}

// segmentMaxTimestamp scans the segment's records and returns the greatest
// timestamp found. The boolean is false if the segment contains no records.
func segmentMaxTimestamp(seg *segment) (int64, bool, error) {
	var maxTimestamp int64
	found := false

	for _, position := range seg.positions {
		_, timestamp, err := readRecordAt(seg, position)
		if err != nil {
			return 0, false, err
		}

		if !found || timestamp > maxTimestamp {
			maxTimestamp = timestamp
			found = true
		}
	}

	return maxTimestamp, found, nil
}

// segmentStorageSize includes the log and both index sidecars.
// A missing sidecar is treated as zero bytes; a missing log is an error.
func segmentStorageSize(seg *segment) (int64, error) {
	base := strings.TrimSuffix(seg.path, ".log")
	paths := []string{
		seg.path,
		base + ".index",
		base + ".timeindex",
	}

	var total int64

	for i, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && i > 0 {
				continue
			}
			return 0, err
		}

		if !info.Mode().IsRegular() {
			return 0, fmt.Errorf("expected regular file at %q", path)
		}

		total += info.Size()
	}

	return total, nil
}

// deleteSegmentFiles removes index sidecars first and the .log file last.
// The .log file is the authoritative segment data: while it exists, recovery
// can rebuild its indexes.
//
// If deletion fails before the log file is removed, this function attempts
// to reopen the segment's handles and rebuild the sidecar indexes.
func deleteSegmentFiles(seg *segment) error {
	base := strings.TrimSuffix(seg.path, ".log")
	indexPath := base + ".index"
	timeIndexPath := base + ".timeindex"

	if err := closeSegmentFiles(seg); err != nil {
		return fmt.Errorf("close segment files: %w", err)
	}

	// Remove sidecars before removing the authoritative log.
	if err := removeIfExists(indexPath); err != nil {
		restoreErr := reopenSegmentFiles(seg)
		return errors.Join(
			fmt.Errorf("remove offset index: %w", err),
			restoreErr,
		)
	}

	if err := removeIfExists(timeIndexPath); err != nil {
		restoreErr := reopenSegmentFiles(seg)
		return errors.Join(
			fmt.Errorf("remove time index: %w", err),
			restoreErr,
		)
	}

	// Once this succeeds, the segment is deleted. If it fails, restore handles
	// and rebuild sidecars where possible.
	if err := os.Remove(seg.path); err != nil {
		restoreErr := reopenSegmentFiles(seg)
		return errors.Join(
			fmt.Errorf("remove log file: %w", err),
			restoreErr,
		)
	}

	return nil
}

func removeIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// closeSegmentFiles closes any currently open segment handles and clears the
// fields so a subsequent restoration can safely reopen them.
func closeSegmentFiles(seg *segment) error {
	var first error

	files := []*os.File{
		seg.file,
		seg.indexFile,
		seg.timeIndexFile,
	}

	for _, f := range files {
		if f == nil {
			continue
		}
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
	}

	seg.file = nil
	seg.indexFile = nil
	seg.timeIndexFile = nil

	if first != nil {
		return first
	}

	return nil
}

// reopenSegmentFiles reopens a segment and its sidecars, then rebuilds both
// indexes from the authoritative log file.
func reopenSegmentFiles(seg *segment) error {
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("reopen log file: %w", err)
	}

	base := strings.TrimSuffix(seg.path, ".log")

	idx, err := os.OpenFile(base+".index", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		f.Close()
		return fmt.Errorf("reopen offset index: %w", err)
	}

	tidx, err := os.OpenFile(base+".timeindex", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		f.Close()
		idx.Close()
		return fmt.Errorf("reopen time index: %w", err)
	}

	seg.file = f
	seg.indexFile = idx
	seg.timeIndexFile = tidx

	// Rebuild from scratch to restore sidecars if one was removed or damaged.
	seg.positions = nil
	seg.indexEntries = nil
	seg.timeIndexEntries = nil
	seg.size = 0

	if err := scanSegment(seg); err != nil {
		closeSegmentFiles(seg)
		return fmt.Errorf("rescan segment: %w", err)
	}

	if err := rebuildIndexes(seg); err != nil {
		closeSegmentFiles(seg)
		return fmt.Errorf("rebuild indexes: %w", err)
	}

	if _, err := seg.file.Seek(0, 2); err != nil {
		closeSegmentFiles(seg)
		return fmt.Errorf("seek log to end: %w", err)
	}

	return nil
}
