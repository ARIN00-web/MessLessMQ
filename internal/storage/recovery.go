package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

// recoverSegmentTail repairs an incomplete trailing record in a segment.
//
// Only call this for the active (last) segment during startup. A partial
// header or payload at EOF is consistent with a crash during a write.
// An impossible payload length is treated as corruption, not repaired.
//
// The returned value is the number of bytes removed.
func recoverSegmentTail(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat segment: %w", err)
	}

	fileSize := info.Size()
	var pos int64

	for pos < fileSize {
		remaining := fileSize - pos

		// A record header itself was only partially written.
		if remaining < RecordHeaderSize {
			if err := file.Truncate(pos); err != nil {
				return 0, fmt.Errorf("truncate incomplete record header: %w", err)
			}
			if err := file.Sync(); err != nil {
				return 0, fmt.Errorf("sync repaired segment: %w", err)
			}
			return fileSize - pos, nil
		}

		var header [RecordHeaderSize]byte
		if _, err := file.ReadAt(header[:], pos); err != nil {
			return 0, fmt.Errorf("read record header at byte %d: %w", pos, err)
		}

		payloadLength := binary.BigEndian.Uint32(header[:RecordLengthSize])
		if payloadLength > uint32(MaxRecordSize) {
			return 0, fmt.Errorf(
				"%w: impossible payload length %d at byte %d",
				ErrInvalidRecord,
				payloadLength,
				pos,
			)
		}

		recordSize := int64(RecordHeaderSize) + int64(payloadLength)

		// The header is complete, but its declared payload extends beyond EOF.
		if recordSize > remaining {
			if err := file.Truncate(pos); err != nil {
				return 0, fmt.Errorf("truncate incomplete record payload: %w", err)
			}
			if err := file.Sync(); err != nil {
				return 0, fmt.Errorf("sync repaired segment: %w", err)
			}
			return fileSize - pos, nil
		}

		// This complete record ends at the next record boundary.
		pos += recordSize
	}

	return 0, nil
}
