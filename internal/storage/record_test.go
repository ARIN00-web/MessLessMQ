package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestEncodeDecodeRecord(t *testing.T) {
	payload := []byte("MessLessMQ message")
	timestamp := int64(1_797_000_000_123)

	encoded, err := EncodeRecordAt(payload, timestamp)
	if err != nil {
		t.Fatal(err)
	}

	if got := binary.BigEndian.Uint32(encoded[:RecordLengthSize]); got != uint32(len(payload)) {
		t.Fatalf("encoded length = %d, want %d", got, len(payload))
	}

	decoded, gotTimestamp, err := DecodeRecordWithTimestamp(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decoded, payload) {
		t.Fatalf("payload = %q, want %q", decoded, payload)
	}
	if gotTimestamp != timestamp {
		t.Fatalf("timestamp = %d, want %d", gotTimestamp, timestamp)
	}

	// The original decoding API remains compatible.
	decoded, err = DecodeRecord(encoded)
	if err != nil || !bytes.Equal(decoded, payload) {
		t.Fatalf("DecodeRecord() = %q, error = %v", decoded, err)
	}
}

func TestRecordSupportsEmptyPayload(t *testing.T) {
	encoded, err := EncodeRecordAt(nil, 1234)
	if err != nil {
		t.Fatal(err)
	}

	payload, timestamp, err := DecodeRecordWithTimestamp(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) != 0 {
		t.Fatalf("payload length = %d, want 0", len(payload))
	}
	if timestamp != 1234 {
		t.Fatalf("timestamp = %d, want 1234", timestamp)
	}
}

func TestDecodeRecordRejectsIncompleteHeader(t *testing.T) {
	_, err := DecodeRecord([]byte{0, 0})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

func TestDecodeRecordRejectsLengthMismatch(t *testing.T) {
	record, err := EncodeRecordAt([]byte("hi"), 123)
	if err != nil {
		t.Fatal(err)
	}
	record = append(record, 'x')

	_, err = DecodeRecord(record)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

func TestDecodeRecordRejectsOversizedPayload(t *testing.T) {
	record := make([]byte, RecordHeaderSize)
	binary.BigEndian.PutUint32(record[:RecordLengthSize], uint32(MaxRecordSize+1))

	_, err := DecodeRecord(record)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

func TestEncodeRecordRejectsOversizedPayload(t *testing.T) {
	_, err := EncodeRecord(make([]byte, MaxRecordSize+1))
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}
