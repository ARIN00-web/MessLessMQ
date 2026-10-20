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
	"time"
)

var (
	ErrInvalidRecord     = errors.New("invalid log record")
	ErrOffsetNotFound    = errors.New("log offset not found")
	ErrInvalidSegment    = errors.New("invalid log segment")
	ErrTimestampNotFound = errors.New("no record found at or after timestamp")
)

const (
	DefaultSegmentSize   int64  = 1 << 30
	OffsetIndexInterval  uint64 = 128
	OffsetIndexEntrySize        = 16
	TimeIndexEntrySize          = 24
)

type indexEntry struct {
	offset   uint64
	position int64
}

type timeIndexEntry struct {
	maxTimestamp int64
	offset       uint64
	position     int64
}

func encodeIndexEntry(e indexEntry) []byte {
	b := make([]byte, OffsetIndexEntrySize)
	binary.BigEndian.PutUint64(b[:8], e.offset)
	binary.BigEndian.PutUint64(b[8:], uint64(e.position))
	return b
}

func encodeTimeIndexEntry(e timeIndexEntry) []byte {
	b := make([]byte, TimeIndexEntrySize)
	binary.BigEndian.PutUint64(b[:8], uint64(e.maxTimestamp))
	binary.BigEndian.PutUint64(b[8:16], e.offset)
	binary.BigEndian.PutUint64(b[16:24], uint64(e.position))
	return b
}

type segment struct {
	baseOffset       uint64
	file             *os.File
	indexFile        *os.File
	timeIndexFile    *os.File
	positions        []int64
	indexEntries     []indexEntry
	timeIndexEntries []timeIndexEntry
	size             int64
	path             string
}

type Log struct {
	mu              sync.RWMutex
	dir             string
	segments        []*segment
	nextOffset      uint64
	maxSegmentBytes int64
	closed          bool
}

func OpenLog(path string) (*Log, error) {
	return OpenLogWithSegmentSize(path, DefaultSegmentSize)
}

func OpenLogWithSegmentSize(path string, maxSegmentBytes int64) (*Log, error) {
	if maxSegmentBytes <= 0 {
		return nil, fmt.Errorf("segment size must be positive")
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}

	l := &Log{dir: path, maxSegmentBytes: maxSegmentBytes}
	if err := l.loadSegments(); err != nil {
		l.closeFiles()
		return nil, err
	}
	if len(l.segments) == 0 {
		if err := l.createSegment(0); err != nil {
			l.closeFiles()
			return nil, err
		}
	}
	return l, nil
}

func segmentFilename(base uint64) string {
	return fmt.Sprintf("%020d.log", base)
}

func segmentIndexFilename(base uint64) string {
	return fmt.Sprintf("%020d.index", base)
}

func segmentTimeIndexFilename(base uint64) string {
	return fmt.Sprintf("%020d.timeindex", base)
}

func (l *Log) loadSegments() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("read log directory: %w", err)
	}

	type fileInfo struct {
		base uint64
		path string
	}
	var files []fileInfo

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}
		var base uint64
		if _, err := fmt.Sscanf(entry.Name(), "%020d.log", &base); err != nil ||
			entry.Name() != segmentFilename(base) {
			return fmt.Errorf("%w: invalid segment filename %q", ErrInvalidSegment, entry.Name())
		}
		files = append(files, fileInfo{base, filepath.Join(l.dir, entry.Name())})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].base < files[j].base })

	var expected uint64
	for _, sf := range files {
		if sf.base != expected {
			return fmt.Errorf("%w: segment starts at %d, expected %d", ErrInvalidSegment, sf.base, expected)
		}

		f, err := os.OpenFile(sf.path, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("open segment: %w", err)
		}

		openSidecar := func(name string) (*os.File, error) {
			return os.OpenFile(filepath.Join(l.dir, name), os.O_CREATE|os.O_RDWR, 0644)
		}
		idx, err := openSidecar(segmentIndexFilename(sf.base))
		if err != nil {
			f.Close()
			return err
		}
		tidx, err := openSidecar(segmentTimeIndexFilename(sf.base))
		if err != nil {
			f.Close()
			idx.Close()
			return err
		}

		seg := &segment{
			baseOffset:    sf.base,
			file:          f,
			indexFile:     idx,
			timeIndexFile: tidx,
			path:          sf.path,
		}
		if err := scanSegment(seg); err != nil {
			f.Close()
			idx.Close()
			tidx.Close()
			return fmt.Errorf("scan segment %s: %w", sf.path, err)
		}
		if err := rebuildIndexes(seg); err != nil {
			f.Close()
			idx.Close()
			tidx.Close()
			return fmt.Errorf("rebuild indexes for %s: %w", sf.path, err)
		}

		expected = seg.baseOffset + uint64(len(seg.positions))
		l.segments = append(l.segments, seg)
	}
	l.nextOffset = expected
	return nil
}

func scanSegment(seg *segment) error {
	info, err := seg.file.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	var pos int64

	for pos < fileSize {
		if fileSize-pos < RecordHeaderSize {
			return fmt.Errorf("%w: incomplete header at byte %d", ErrInvalidRecord, pos)
		}
		var header [RecordHeaderSize]byte
		if _, err := seg.file.ReadAt(header[:], pos); err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(header[:RecordLengthSize])
		if length > uint32(MaxRecordSize) {
			return fmt.Errorf("%w: payload exceeds maximum", ErrInvalidRecord)
		}
		start := pos
		pos += RecordHeaderSize
		if int64(length) > fileSize-pos {
			return fmt.Errorf("%w: record at byte %d extends beyond segment", ErrInvalidRecord, start)
		}

		payload := make([]byte, int(length))
		if _, err := seg.file.ReadAt(payload, pos); err != nil {
			return err
		}
		timestamp := int64(binary.BigEndian.Uint64(header[RecordLengthSize:RecordHeaderSize]))

		local := uint64(len(seg.positions))
		seg.positions = append(seg.positions, start)
		if local%OffsetIndexInterval == 0 {
			seg.indexEntries = append(seg.indexEntries, indexEntry{
				offset: seg.baseOffset + local, position: start,
			})
			seg.timeIndexEntries = append(seg.timeIndexEntries, timeIndexEntry{
				maxTimestamp: timestamp,
				offset:       seg.baseOffset + local,
				position:     start,
			})
		} else {
			block := len(seg.timeIndexEntries) - 1
			if timestamp > seg.timeIndexEntries[block].maxTimestamp {
				seg.timeIndexEntries[block].maxTimestamp = timestamp
			}
		}
		pos += int64(length)
	}
	seg.size = fileSize
	return nil
}

func rebuildIndexes(seg *segment) error {
	if err := seg.indexFile.Truncate(0); err != nil {
		return err
	}
	if _, err := seg.indexFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for _, e := range seg.indexEntries {
		if err := writeFull(seg.indexFile, encodeIndexEntry(e)); err != nil {
			return err
		}
	}

	if err := seg.timeIndexFile.Truncate(0); err != nil {
		return err
	}
	if _, err := seg.timeIndexFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for _, e := range seg.timeIndexEntries {
		if err := writeFull(seg.timeIndexFile, encodeTimeIndexEntry(e)); err != nil {
			return err
		}
	}
	_, err := seg.timeIndexFile.Seek(0, io.SeekEnd)
	return err
}

func (l *Log) createSegment(base uint64) error {
	path := filepath.Join(l.dir, segmentFilename(base))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("create segment: %w", err)
	}

	openSidecar := func(name string) (*os.File, error) {
		return os.OpenFile(filepath.Join(l.dir, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	}
	idx, err := openSidecar(segmentIndexFilename(base))
	if err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	tidx, err := openSidecar(segmentTimeIndexFilename(base))
	if err != nil {
		f.Close()
		idx.Close()
		os.Remove(path)
		os.Remove(filepath.Join(l.dir, segmentIndexFilename(base)))
		return err
	}

	l.segments = append(l.segments, &segment{
		baseOffset: base, file: f, indexFile: idx, timeIndexFile: tidx, path: path,
	})
	return nil
}

func (l *Log) Append(data []byte) (uint64, error) {
	return l.AppendAt(data, time.Now().UnixMilli())
}

func (l *Log) AppendAt(data []byte, timestamp int64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, os.ErrClosed
	}
	record, err := EncodeRecordAt(data, timestamp)
	if err != nil {
		return 0, err
	}

	active := l.segments[len(l.segments)-1]
	if active.size > 0 && active.size+int64(len(record)) > l.maxSegmentBytes {
		if err := l.createSegment(l.nextOffset); err != nil {
			return 0, err
		}
		active = l.segments[len(l.segments)-1]
	}

	position, err := active.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if err := writeFull(active.file, record); err != nil {
		return 0, fmt.Errorf("write record: %w", err)
	}

	offset := l.nextOffset
	local := uint64(len(active.positions))
	active.positions = append(active.positions, position)
	active.size += int64(len(record))
	l.nextOffset++

	if local%OffsetIndexInterval == 0 {
		entry := indexEntry{offset: offset, position: position}
		timeEntry := timeIndexEntry{
			maxTimestamp: timestamp, offset: offset, position: position,
		}
		if err := writeFull(active.indexFile, encodeIndexEntry(entry)); err != nil {
			return offset, fmt.Errorf("write offset index: %w", err)
		}
		if err := writeFull(active.timeIndexFile, encodeTimeIndexEntry(timeEntry)); err != nil {
			return offset, fmt.Errorf("write time index: %w", err)
		}
		active.indexEntries = append(active.indexEntries, entry)
		active.timeIndexEntries = append(active.timeIndexEntries, timeEntry)
	} else {
		block := len(active.timeIndexEntries) - 1

		if timestamp > active.timeIndexEntries[block].maxTimestamp {
			active.timeIndexEntries[block].maxTimestamp = timestamp

			encoded := encodeTimeIndexEntry(active.timeIndexEntries[block])
			n, err := active.timeIndexFile.WriteAt(
				encoded,
				int64(block*TimeIndexEntrySize),
			)
			if err != nil {
				return offset, fmt.Errorf("update time index: %w", err)
			}
			if n != len(encoded) {
				return offset, fmt.Errorf(
					"update time index: short write: wrote %d of %d bytes",
					n, len(encoded),
				)
			}
		}
	}

	return offset, nil
}

func (l *Log) Read(offset uint64) ([]byte, error) {
	payload, _, err := l.ReadWithTimestamp(offset)
	return payload, err
}

func (l *Log) ReadWithTimestamp(offset uint64) ([]byte, int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, 0, os.ErrClosed
	}
	if offset >= l.nextOffset {
		return nil, 0, fmt.Errorf("%w: %d", ErrOffsetNotFound, offset)
	}

	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	}) - 1
	if i < 0 {
		return nil, 0, fmt.Errorf("%w: %d", ErrOffsetNotFound, offset)
	}
	seg := l.segments[i]
	local := offset - seg.baseOffset
	if local >= uint64(len(seg.positions)) {
		return nil, 0, fmt.Errorf("%w: %d", ErrOffsetNotFound, offset)
	}
	return readRecordAt(seg, seg.positions[local])
}

// ReadAtOrAfterTimestamp returns the earliest-offset record whose timestamp
// is >= target. Timestamps need not be monotonic.
func (l *Log) ReadAtOrAfterTimestamp(target int64) (uint64, []byte, int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return 0, nil, 0, os.ErrClosed
	}

	for _, seg := range l.segments {
		for i, entry := range seg.timeIndexEntries {
			if entry.maxTimestamp < target {
				continue
			}

			startLocal := entry.offset - seg.baseOffset
			endLocal := uint64(len(seg.positions))
			if i+1 < len(seg.timeIndexEntries) {
				endLocal = seg.timeIndexEntries[i+1].offset - seg.baseOffset
			}

			for local := startLocal; local < endLocal; local++ {
				payload, ts, err := readRecordAt(seg, seg.positions[local])
				if err != nil {
					return 0, nil, 0, err
				}
				if ts >= target {
					return seg.baseOffset + local, payload, ts, nil
				}
			}
		}
	}

	return 0, nil, 0, fmt.Errorf("%w: %d", ErrTimestampNotFound, target)
}

func readRecordAt(seg *segment, position int64) ([]byte, int64, error) {
	var header [RecordHeaderSize]byte
	if _, err := seg.file.ReadAt(header[:], position); err != nil {
		return nil, 0, fmt.Errorf("read record header: %w", err)
	}
	length := binary.BigEndian.Uint32(header[:RecordLengthSize])
	if length > uint32(MaxRecordSize) {
		return nil, 0, fmt.Errorf("%w: payload exceeds maximum", ErrInvalidRecord)
	}
	timestamp := int64(binary.BigEndian.Uint64(header[RecordLengthSize:RecordHeaderSize]))
	payload := make([]byte, int(length))
	if _, err := seg.file.ReadAt(payload, position+RecordHeaderSize); err != nil {
		return nil, 0, fmt.Errorf("read record payload: %w", err)
	}
	return payload, timestamp, nil
}

func (l *Log) NextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}

func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return os.ErrClosed
	}
	for _, seg := range l.segments {
		for _, f := range []*os.File{seg.file, seg.indexFile, seg.timeIndexFile} {
			if err := f.Sync(); err != nil {
				return fmt.Errorf("sync %s: %w", seg.path, err)
			}
		}
	}
	return nil
}

func (l *Log) AppendSync(data []byte) (uint64, error) {
	offset, err := l.Append(data)
	if err != nil {
		return 0, err
	}
	if err := l.Sync(); err != nil {
		return offset, fmt.Errorf("sync after append at offset %d: %w", offset, err)
	}
	return offset, nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.closeFiles()
}

func (l *Log) closeFiles() error {
	var first error
	for _, seg := range l.segments {
		for _, f := range []*os.File{seg.file, seg.indexFile, seg.timeIndexFile} {
			if f != nil {
				if err := f.Close(); err != nil && first == nil {
					first = err
				}
			}
		}
	}
	if first != nil {
		return fmt.Errorf("close log files: %w", first)
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
