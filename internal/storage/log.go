package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	ErrInvalidRecord  = errors.New("invalid log record")
	ErrOffsetNotFound = errors.New("log offset not found")
)

// Log is a basic append-only file containing length-prefixed records.
//
// Offsets are logical record numbers starting at zero.
// Record positions are maintained in memory.
type Log struct {
	mu         sync.RWMutex
	file       *os.File
	nextOffset uint64
	positions  []int64
}

// OpenLog opens or creates a log and scans existing records.
func OpenLog(path string) (*Log, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}

	log := &Log{
		file: file,
	}

	if err := log.loadExistingRecords(); err != nil {
		_ = file.Close()
		return nil, err
	}

	return log, nil
}

// loadExistingRecords reconstructs record positions and the next offset.
// Incomplete or malformed records are rejected rather than repaired.
func (l *Log) loadExistingRecords() error {
	info, err := l.file.Stat()
	if err != nil {
		return fmt.Errorf("stat log: %w", err)
	}

	fileSize := info.Size()
	var position int64

	for position < fileSize {
		if fileSize-position < RecordHeaderSize {
			return fmt.Errorf(
				"%w: incomplete length header at byte %d",
				ErrInvalidRecord,
				position,
			)
		}

		var header [RecordHeaderSize]byte

		if _, err := l.file.ReadAt(header[:], position); err != nil {
			return fmt.Errorf(
				"read record header at byte %d: %w",
				position,
				err,
			)
		}

		recordStart := position
		recordLength := int64(binary.BigEndian.Uint32(header[:]))

		if recordLength > int64(MaxRecordSize) {
			return fmt.Errorf(
				"%w: record at byte %d exceeds maximum size",
				ErrInvalidRecord,
				recordStart,
			)
		}

		position += RecordHeaderSize

		if recordLength > fileSize-position {
			return fmt.Errorf(
				"%w: record at byte %d declares %d bytes beyond end of file",
				ErrInvalidRecord,
				recordStart,
				recordLength,
			)
		}

		l.positions = append(l.positions, recordStart)
		position += recordLength
	}

	l.nextOffset = uint64(len(l.positions))

	return nil
}

// Append adds one payload and returns its logical offset.
func (l *Log) Append(data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return 0, os.ErrClosed
	}

	record, err := EncodeRecord(data)
	if err != nil {
		return 0, err
	}

	position, err := l.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("seek log end: %w", err)
	}

	if err := writeFull(l.file, record); err != nil {
		return 0, fmt.Errorf("write record: %w", err)
	}

	offset := l.nextOffset
	l.positions = append(l.positions, position)
	l.nextOffset++

	return offset, nil
}

// Read returns a copy of the payload at the specified logical offset.
func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.file == nil {
		return nil, os.ErrClosed
	}

	if offset >= uint64(len(l.positions)) {
		return nil, fmt.Errorf("%w: %d", ErrOffsetNotFound, offset)
	}

	position := l.positions[offset]

	var header [RecordHeaderSize]byte

	if _, err := l.file.ReadAt(header[:], position); err != nil {
		return nil, fmt.Errorf("read record header: %w", err)
	}

	length := binary.BigEndian.Uint32(header[:])

	if length > uint32(MaxRecordSize) {
		return nil, fmt.Errorf(
			"%w: record at offset %d exceeds maximum size",
			ErrInvalidRecord,
			offset,
		)
	}

	data := make([]byte, int(length))

	if _, err := l.file.ReadAt(data, position+RecordHeaderSize); err != nil {
		return nil, fmt.Errorf("read record data: %w", err)
	}

	return data, nil
}

// NextOffset returns the offset that will be assigned to the next record.
func (l *Log) NextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.nextOffset
}

// Close closes the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}

	err := l.file.Close()
	l.file = nil

	if err != nil {
		return fmt.Errorf("close log: %w", err)
	}

	return nil
}

// writeFull keeps writing until all bytes have been written or an error occurs.
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
