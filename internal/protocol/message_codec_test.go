package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	original := Message{
		Topic:     "orders",
		Key:       []byte("order-1042"),
		Value:     []byte(`{"status":"created"}`),
		Timestamp: 1791936000000,
	}

	encoded, err := EncodeMessage(original)
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}

	decoded, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("DecodeMessage failed: %v", err)
	}

	if decoded.Topic != original.Topic {
		t.Fatalf("topic: got %q, want %q", decoded.Topic, original.Topic)
	}

	if !bytes.Equal(decoded.Key, original.Key) {
		t.Fatalf("key: got %q, want %q", decoded.Key, original.Key)
	}

	if !bytes.Equal(decoded.Value, original.Value) {
		t.Fatalf("value: got %q, want %q", decoded.Value, original.Value)
	}

	if decoded.Timestamp != original.Timestamp {
		t.Fatalf("timestamp: got %d, want %d",
			decoded.Timestamp, original.Timestamp)
	}
}

func TestMessageWithNilKey(t *testing.T) {
	original := Message{
		Topic:     "events",
		Key:       nil,
		Value:     []byte("hello"),
		Timestamp: 1000,
	}

	encoded, err := EncodeMessage(original)
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}

	decoded, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("DecodeMessage failed: %v", err)
	}

	if decoded.Key != nil {
		t.Fatalf("expected nil key, got %#v", decoded.Key)
	}
}

func TestMessageWithEmptyKey(t *testing.T) {
	original := Message{
		Topic:     "events",
		Key:       []byte{},
		Value:     []byte("hello"),
		Timestamp: 1000,
	}

	encoded, err := EncodeMessage(original)
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}

	decoded, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("DecodeMessage failed: %v", err)
	}

	if decoded.Key == nil {
		t.Fatal("expected non-nil empty key")
	}

	if len(decoded.Key) != 0 {
		t.Fatalf("expected empty key, got %q", decoded.Key)
	}
}

func TestDecodeMessageRejectsShortHeader(t *testing.T) {
	_, err := DecodeMessage([]byte{0x01, 0x02})
	if err == nil {
		t.Fatal("expected error for short message header")
	}
}

func TestDecodeMessageRejectsInvalidLengths(t *testing.T) {
	data := make([]byte, messageHeaderSize)

	binary.BigEndian.PutUint16(data[0:2], 1) // topic length
	binary.BigEndian.PutUint32(data[2:6], 0xFFFFFFFE)

	_, err := DecodeMessage(data)
	if err == nil {
		t.Fatal("expected error for invalid key length")
	}
}

func TestDecodeMessageRejectsInconsistentSize(t *testing.T) {
	encoded, err := EncodeMessage(Message{
		Topic: "events",
		Value: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}

	// Remove one byte while leaving the declared lengths unchanged.
	_, err = DecodeMessage(encoded[:len(encoded)-1])
	if err == nil {
		t.Fatal("expected error for inconsistent message size")
	}
}

func TestEncodeMessageRejectsEmptyTopic(t *testing.T) {
	_, err := EncodeMessage(Message{
		Value: []byte("hello"),
	})

	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}
