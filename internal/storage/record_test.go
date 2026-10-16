package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestEncodeDecodeRecord(t *testing.T) {
	payload := []byte("MessLessMQ message")

	encoded, err := EncodeRecord(payload)
	if err != nil {
		t.Fatalf("EncodeRecord() error: %v", err)
	}

	if got := binary.BigEndian.Uint32(encoded[:RecordHeaderSize]); got != uint32(len(payload)) {
		t.Fatalf("encoded length = %d, want %d", got, len(payload))
	}

	decoded, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeRecord() error: %v", err)
	}

	if !bytes.Equal(decoded, payload) {
		t.Fatalf("decoded payload = %q, want %q", decoded, payload)
	}
}

func TestRecordSupportsEmptyPayload(t *testing.T) {
	encoded, err := EncodeRecord(nil)
	if err != nil {
		t.Fatalf("EncodeRecord() error: %v", err)
	}

	decoded, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeRecord() error: %v", err)
	}

	if len(decoded) != 0 {
		t.Fatalf("decoded payload length = %d, want 0", len(decoded))
	}
}

func TestDecodeRecordRejectsIncompleteHeader(t *testing.T) {
	_, err := DecodeRecord([]byte{0, 0})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

func TestDecodeRecordRejectsLengthMismatch(t *testing.T) {
	// Header declares 5 payload bytes, but only 2 are present.
	record := []byte{0, 0, 0, 5, 'h', 'i'}

	_, err := DecodeRecord(record)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

func TestDecodeRecordRejectsOversizedPayload(t *testing.T) {
	record := make([]byte, RecordHeaderSize)
	binary.BigEndian.PutUint32(record, uint32(MaxRecordSize+1))

	_, err := DecodeRecord(record)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

func TestEncodeRecordRejectsOversizedPayload(t *testing.T) {
	payload := make([]byte, MaxRecordSize+1)

	_, err := EncodeRecord(payload)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}
