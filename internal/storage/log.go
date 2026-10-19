package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	ErrInvalidRecord  = errors.New("invalid log record")
	ErrOffsetNotFound = errors.New("log offset not found")
	ErrInvalidSegment = errors.New("invalid log segment")
)

const DefaultSegmentSize int64 = 1 << 30 // 1 GiB

type segment struct {
	baseOffset uint64
	file       *os.File
	positions  []int64
	size       int64
	path       string
}

// Log is a logical append-only log split across segment files.
type Log struct {
	mu              sync.RWMutex
	dir             string
	segments        []*segment
	nextOffset      uint64
	maxSegmentBytes int64
	closed          bool
}

// OpenLog opens or creates a segmented log directory.
func OpenLog(path string) (*Log, error) {
	return OpenLogWithSegmentSize(path, DefaultSegmentSize)
}

// OpenLogWithSegmentSize allows tests to use smaller segments.
func OpenLogWithSegmentSize(path string, maxSegmentBytes int64) (*Log, error) {
	if maxSegmentBytes <= 0 {
		return nil, fmt.Errorf("segment size must be positive")
	}

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}

	l := &Log{
		dir:             path,
		maxSegmentBytes: maxSegmentBytes,
	}

	if err := l.loadSegments(); err != nil {
		for _, seg := range l.segments {
			_ = seg.file.Close()
		}
		return nil, err
	}

	if len(l.segments) == 0 {
		if err := l.createSegment(0); err != nil {
			return nil, err
		}
	}

	return l, nil
}

func segmentFilename(baseOffset uint64) string {
	return fmt.Sprintf("%020d.log", baseOffset)
}

func (l *Log) loadSegments() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("read log directory: %w", err)
	}

	type segmentFile struct {
		base uint64
		path string
	}

	var files []segmentFile

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}

		var base uint64
		if _, err := fmt.Sscanf(entry.Name(), "%020d.log", &base); err != nil {
			return fmt.Errorf(
				"%w: invalid segment filename %q",
				ErrInvalidSegment,
				entry.Name(),
			)
		}

		if entry.Name() != segmentFilename(base) {
			return fmt.Errorf(
				"%w: invalid segment filename %q",
				ErrInvalidSegment,
				entry.Name(),
			)
		}

		files = append(files, segmentFile{
			base: base,
			path: filepath.Join(l.dir, entry.Name()),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].base < files[j].base
	})

	var expectedOffset uint64

	for _, sf := range files {
		if sf.base != expectedOffset {
			return fmt.Errorf(
				"%w: segment starts at offset %d, expected %d",
				ErrInvalidSegment,
				sf.base,
				expectedOffset,
			)
		}

		f, err := os.OpenFile(sf.path, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("open segment %s: %w", sf.path, err)
		}

		seg := &segment{
			baseOffset: sf.base,
			file:       f,
			path:       sf.path,
		}

		if err := scanSegment(seg); err != nil {
			_ = f.Close()
			return fmt.Errorf("scan segment %s: %w", sf.path, err)
		}

		if int64(len(seg.positions)) > 0 &&
			seg.baseOffset+uint64(len(seg.positions)) < expectedOffset {
			_ = f.Close()
			return fmt.Errorf("%w: inconsistent segment offsets", ErrInvalidSegment)
		}

		expectedOffset = seg.baseOffset + uint64(len(seg.positions))
		l.segments = append(l.segments, seg)
	}

	l.nextOffset = expectedOffset
	return nil
}

func scanSegment(seg *segment) error {
	info, err := seg.file.Stat()
	if err != nil {
		return fmt.Errorf("stat segment: %w", err)
	}

	fileSize := info.Size()
	var position int64

	for position < fileSize {
		if fileSize-position < RecordHeaderSize {
			return fmt.Errorf(
				"%w: incomplete record header at byte %d",
				ErrInvalidRecord,
				position,
			)
		}

		var header [RecordHeaderSize]byte
		if _, err := seg.file.ReadAt(header[:], position); err != nil {
			return fmt.Errorf("read record header: %w", err)
		}

		length := int64(binary.BigEndian.Uint32(header[:]))
		if length > int64(MaxRecordSize) {
			return fmt.Errorf("%w: record exceeds maximum size", ErrInvalidRecord)
		}

		recordStart := position
		position += RecordHeaderSize

		if length > fileSize-position {
			return fmt.Errorf(
				"%w: record at byte %d extends beyond segment",
				ErrInvalidRecord,
				recordStart,
			)
		}

		seg.positions = append(seg.positions, recordStart)
		position += length
	}

	seg.size = fileSize
	return nil
}

func (l *Log) createSegment(baseOffset uint64) error {
	path := filepath.Join(l.dir, segmentFilename(baseOffset))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("create segment %s: %w", path, err)
	}

	l.segments = append(l.segments, &segment{
		baseOffset: baseOffset,
		file:       f,
		path:       path,
	})

	return nil
}

// Append adds a payload and returns its logical offset.
func (l *Log) Append(data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, os.ErrClosed
	}

	record, err := EncodeRecord(data)
	if err != nil {
		return 0, err
	}

	active := l.segments[len(l.segments)-1]

	if active.size > 0 &&
		active.size+int64(len(record)) > l.maxSegmentBytes {
		if err := l.createSegment(l.nextOffset); err != nil {
			return 0, err
		}
		active = l.segments[len(l.segments)-1]
	}

	position, err := active.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("seek segment end: %w", err)
	}

	if err := writeFull(active.file, record); err != nil {
		return 0, fmt.Errorf("write record: %w", err)
	}

	offset := l.nextOffset
	active.positions = append(active.positions, position)
	active.size += int64(len(record))
	l.nextOffset++

	return offset, nil
}

// Read returns the payload at a logical offset.
func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, os.ErrClosed
	}

	if offset >= l.nextOffset {
		return nil, fmt.Errorf("%w: %d", ErrOffsetNotFound, offset)
	}

	segIndex := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	}) - 1

	if segIndex < 0 {
		return nil, fmt.Errorf("%w: %d", ErrOffsetNotFound, offset)
	}

	seg := l.segments[segIndex]
	localOffset := offset - seg.baseOffset

	if localOffset >= uint64(len(seg.positions)) {
		return nil, fmt.Errorf("%w: %d", ErrOffsetNotFound, offset)
	}

	position := seg.positions[localOffset]

	var header [RecordHeaderSize]byte
	if _, err := seg.file.ReadAt(header[:], position); err != nil {
		return nil, fmt.Errorf("read record header: %w", err)
	}

	length := binary.BigEndian.Uint32(header[:])
	if length > uint32(MaxRecordSize) {
		return nil, fmt.Errorf("%w: record exceeds maximum size", ErrInvalidRecord)
	}

	payload := make([]byte, int(length))
	if _, err := seg.file.ReadAt(payload, position+RecordHeaderSize); err != nil {
		return nil, fmt.Errorf("read record payload: %w", err)
	}

	return payload, nil
}

// NextOffset returns the next logical offset.
func (l *Log) NextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}

// Close closes all segment files.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true

	var firstErr error
	for _, seg := range l.segments {
		if err := seg.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return fmt.Errorf("close segment: %w", firstErr)
	}

	return nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}

	return nil
}
