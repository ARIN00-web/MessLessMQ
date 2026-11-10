package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

var ErrCorruptIndex = errors.New("corrupt storage index")

// VerificationReport describes the structural verification of one log.
// Verification checks record boundaries, segment continuity, offsets, and
// sidecar index bytes. The current record format has no payload checksum, so
// same-length payload bit flips cannot be detected by this report.
type VerificationReport struct {
	Segments    int
	Records     uint64
	Bytes       int64
	StartOffset uint64
	NextOffset  uint64
}

// Verify checks the currently open log without modifying it.
//
// It verifies that:
//   - segment base offsets form a contiguous range;
//   - each record begins at the position recorded in memory;
//   - every record header and payload is structurally complete;
//   - each segment's recorded size matches its file size;
//   - offset and time index sidecars match their in-memory entries; and
//   - the recovered next offset matches the end of the final segment.
//
// Verify does not repair files. OpenLog performs crash-tail repair and index
// rebuilding during startup; call Verify after opening to validate the result.
func (l *Log) Verify() (VerificationReport, error) {
	if l == nil {
		return VerificationReport{}, os.ErrInvalid
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return VerificationReport{}, os.ErrClosed
	}

	report := VerificationReport{
		Segments:   len(l.segments),
		NextOffset: l.nextOffset,
	}

	if len(l.segments) == 0 {
		report.StartOffset = l.nextOffset
		return report, nil
	}

	expectedOffset := l.segments[0].baseOffset
	report.StartOffset = expectedOffset

	for segmentIndex, seg := range l.segments {
		if seg == nil || seg.file == nil || seg.indexFile == nil || seg.timeIndexFile == nil {
			return report, fmt.Errorf("%w: segment %d has a nil file handle", ErrInvalidSegment, segmentIndex)
		}

		if seg.baseOffset != expectedOffset {
			return report, fmt.Errorf(
				"%w: segment %d starts at offset %d, expected %d",
				ErrInvalidSegment,
				segmentIndex,
				seg.baseOffset,
				expectedOffset,
			)
		}

		info, err := seg.file.Stat()
		if err != nil {
			return report, fmt.Errorf("stat segment %s: %w", seg.path, err)
		}
		if info.Size() != seg.size {
			return report, fmt.Errorf(
				"%w: segment %s size is %d, in-memory size is %d",
				ErrInvalidSegment,
				seg.path,
				info.Size(),
				seg.size,
			)
		}

		var expectedPosition int64
		for localOffset, position := range seg.positions {
			if position != expectedPosition {
				return report, fmt.Errorf(
					"%w: segment %s record %d starts at byte %d, expected %d",
					ErrInvalidRecord,
					seg.path,
					localOffset,
					position,
					expectedPosition,
				)
			}

			var header [RecordHeaderSize]byte
			if _, err := seg.file.ReadAt(header[:], position); err != nil {
				return report, fmt.Errorf("verify header in %s at byte %d: %w", seg.path, position, err)
			}

			payloadLength := binary.BigEndian.Uint32(header[:RecordLengthSize])
			if payloadLength > uint32(MaxRecordSize) {
				return report, fmt.Errorf(
					"%w: payload length %d exceeds maximum in %s at byte %d",
					ErrInvalidRecord,
					payloadLength,
					seg.path,
					position,
				)
			}

			if _, _, err := readRecordAt(seg, position); err != nil {
				return report, fmt.Errorf("verify record offset %d: %w", seg.baseOffset+uint64(localOffset), err)
			}

			recordBytes := int64(RecordHeaderSize) + int64(payloadLength)
			if recordBytes > info.Size()-position {
				return report, fmt.Errorf(
					"%w: record at byte %d extends beyond segment %s",
					ErrInvalidRecord,
					position,
					seg.path,
				)
			}

			expectedPosition += recordBytes
			report.Records++
		}

		if expectedPosition != info.Size() {
			return report, fmt.Errorf(
				"%w: segment %s has %d unindexed trailing bytes",
				ErrInvalidRecord,
				seg.path,
				info.Size()-expectedPosition,
			)
		}

		if err := verifyOffsetIndex(seg); err != nil {
			return report, err
		}
		if err := verifyTimeIndex(seg); err != nil {
			return report, err
		}

		report.Bytes += info.Size()
		expectedOffset = seg.baseOffset + uint64(len(seg.positions))
	}

	if expectedOffset != l.nextOffset {
		return report, fmt.Errorf(
			"%w: log end offset is %d, expected %d",
			ErrInvalidSegment,
			l.nextOffset,
			expectedOffset,
		)
	}

	return report, nil
}

func verifyOffsetIndex(seg *segment) error {
	return verifyIndexFile(
		seg.indexFile,
		seg.indexFile.Name(),
		len(seg.indexEntries)*OffsetIndexEntrySize,
		func(i int) []byte { return encodeIndexEntry(seg.indexEntries[i]) },
		len(seg.indexEntries),
	)
}

func verifyTimeIndex(seg *segment) error {
	return verifyIndexFile(
		seg.timeIndexFile,
		seg.timeIndexFile.Name(),
		len(seg.timeIndexEntries)*TimeIndexEntrySize,
		func(i int) []byte { return encodeTimeIndexEntry(seg.timeIndexEntries[i]) },
		len(seg.timeIndexEntries),
	)
}

func verifyIndexFile(
	file *os.File,
	name string,
	expectedSize int,
	encode func(int) []byte,
	entries int,
) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat index %s: %w", name, err)
	}
	if info.Size() != int64(expectedSize) {
		return fmt.Errorf(
			"%w: %s size is %d, expected %d",
			ErrCorruptIndex,
			name,
			info.Size(),
			expectedSize,
		)
	}

	var position int64
	for i := 0; i < entries; i++ {
		want := encode(i)
		got := make([]byte, len(want))
		if _, err := file.ReadAt(got, position); err != nil {
			return fmt.Errorf("read index %s: %w", name, err)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("%w: entry %d differs in %s", ErrCorruptIndex, i, name)
		}
		position += int64(len(want))
	}

	return nil
}
