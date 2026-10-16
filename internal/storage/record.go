package storage

import (
	"encoding/binary"
	"fmt"
)

const (
	// RecordHeaderSize is the size of the payload-length prefix.
	RecordHeaderSize = 4

	// MaxRecordSize is the maximum size of one record's payload.
	MaxRecordSize = 10 * 1024 * 1024 // 10 MiB
)

// EncodeRecord encodes a payload as a length-prefixed storage record.
//
// Format:
//
//	[Payload length: 4 bytes, big-endian]
//	[Payload: variable length]
func EncodeRecord(payload []byte) ([]byte, error) {
	if len(payload) > MaxRecordSize {
		return nil, fmt.Errorf(
			"%w: payload size %d exceeds maximum %d",
			ErrInvalidRecord,
			len(payload),
			MaxRecordSize,
		)
	}

	record := make([]byte, RecordHeaderSize+len(payload))

	binary.BigEndian.PutUint32(
		record[:RecordHeaderSize],
		uint32(len(payload)),
	)

	copy(record[RecordHeaderSize:], payload)

	return record, nil
}

// DecodeRecord validates and decodes exactly one complete storage record.
func DecodeRecord(record []byte) ([]byte, error) {
	if len(record) < RecordHeaderSize {
		return nil, fmt.Errorf(
			"%w: incomplete record header",
			ErrInvalidRecord,
		)
	}

	payloadLength := binary.BigEndian.Uint32(
		record[:RecordHeaderSize],
	)

	if payloadLength > uint32(MaxRecordSize) {
		return nil, fmt.Errorf(
			"%w: payload size %d exceeds maximum %d",
			ErrInvalidRecord,
			payloadLength,
			MaxRecordSize,
		)
	}

	actualLength := len(record) - RecordHeaderSize

	if uint64(payloadLength) != uint64(actualLength) {
		return nil, fmt.Errorf(
			"%w: declared payload length %d, actual length %d",
			ErrInvalidRecord,
			payloadLength,
			actualLength,
		)
	}

	payload := make([]byte, actualLength)
	copy(payload, record[RecordHeaderSize:])

	return payload, nil
}
