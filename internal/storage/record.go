package storage

import (
	"encoding/binary"
	"fmt"
	"time"
)

const (
	RecordLengthSize    = 4
	RecordTimestampSize = 8
	RecordHeaderSize    = RecordLengthSize + RecordTimestampSize
	MaxRecordSize       = 10 * 1024 * 1024
)

// EncodeRecord preserves the existing API and assigns the current timestamp.
func EncodeRecord(payload []byte) ([]byte, error) {
	return EncodeRecordAt(payload, time.Now().UnixMilli())
}

// EncodeRecordAt encodes a payload with an explicit Unix-millisecond timestamp.
func EncodeRecordAt(payload []byte, timestamp int64) ([]byte, error) {
	if len(payload) > MaxRecordSize {
		return nil, fmt.Errorf(
			"%w: payload size %d exceeds maximum %d",
			ErrInvalidRecord, len(payload), MaxRecordSize,
		)
	}

	record := make([]byte, RecordHeaderSize+len(payload))
	binary.BigEndian.PutUint32(record[:RecordLengthSize], uint32(len(payload)))
	binary.BigEndian.PutUint64(
		record[RecordLengthSize:RecordHeaderSize],
		uint64(timestamp),
	)
	copy(record[RecordHeaderSize:], payload)

	return record, nil
}

// DecodeRecord returns the payload, preserving the existing API.
func DecodeRecord(record []byte) ([]byte, error) {
	payload, _, err := DecodeRecordWithTimestamp(record)
	return payload, err
}

// DecodeRecordWithTimestamp validates a record and returns its payload and timestamp.
func DecodeRecordWithTimestamp(record []byte) ([]byte, int64, error) {
	if len(record) < RecordHeaderSize {
		return nil, 0, fmt.Errorf("%w: incomplete record header", ErrInvalidRecord)
	}

	payloadLength := binary.BigEndian.Uint32(record[:RecordLengthSize])
	if payloadLength > uint32(MaxRecordSize) {
		return nil, 0, fmt.Errorf(
			"%w: payload size %d exceeds maximum %d",
			ErrInvalidRecord, payloadLength, MaxRecordSize,
		)
	}

	actualLength := len(record) - RecordHeaderSize
	if uint64(payloadLength) != uint64(actualLength) {
		return nil, 0, fmt.Errorf(
			"%w: declared payload length %d, actual length %d",
			ErrInvalidRecord, payloadLength, actualLength,
		)
	}

	timestamp := int64(binary.BigEndian.Uint64(
		record[RecordLengthSize:RecordHeaderSize],
	))

	payload := make([]byte, actualLength)
	copy(payload, record[RecordHeaderSize:])

	return payload, timestamp, nil
}
