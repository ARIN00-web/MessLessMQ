package protocol

import (
	"encoding/binary"
	"testing"
)

func TestBatchRoundTrip(t *testing.T) {
	original := MessageBatch{
		Messages: []Message{
			{
				Topic:     "orders",
				Key:       []byte("order-1"),
				Value:     []byte("created"),
				Timestamp: 1000,
			},
			{
				Topic:     "orders",
				Key:       []byte("order-2"),
				Value:     []byte("paid"),
				Timestamp: 2000,
			},
			{
				Topic:     "notifications",
				Key:       nil,
				Value:     []byte("hello"),
				Timestamp: 3000,
			},
		},
	}

	encoded, err := EncodeBatch(original)
	if err != nil {
		t.Fatalf("EncodeBatch failed: %v", err)
	}

	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatalf("DecodeBatch failed: %v", err)
	}

	if len(decoded.Messages) != len(original.Messages) {
		t.Fatalf("got %d messages, want %d",
			len(decoded.Messages), len(original.Messages))
	}

	for i, want := range original.Messages {
		got := decoded.Messages[i]

		if got.Topic != want.Topic {
			t.Errorf("message %d topic: got %q, want %q",
				i, got.Topic, want.Topic)
		}

		if string(got.Key) != string(want.Key) {
			t.Errorf("message %d key: got %q, want %q",
				i, got.Key, want.Key)
		}

		if string(got.Value) != string(want.Value) {
			t.Errorf("message %d value: got %q, want %q",
				i, got.Value, want.Value)
		}

		if got.Timestamp != want.Timestamp {
			t.Errorf("message %d timestamp: got %d, want %d",
				i, got.Timestamp, want.Timestamp)
		}
	}
}

func TestEncodeBatchRejectsEmptyBatch(t *testing.T) {
	_, err := EncodeBatch(MessageBatch{})
	if err == nil {
		t.Fatal("expected empty batch to be rejected")
	}
}

func TestDecodeBatchRejectsEmptyBatch(t *testing.T) {
	data := make([]byte, batchHeaderSize)

	_, err := DecodeBatch(data)
	if err == nil {
		t.Fatal("expected zero-message batch to be rejected")
	}
}

func TestDecodeBatchRejectsTruncatedMessage(t *testing.T) {
	encoded, err := EncodeBatch(MessageBatch{
		Messages: []Message{
			{Topic: "events", Value: []byte("hello")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Remove the final byte but retain the declared lengths.
	_, err = DecodeBatch(encoded[:len(encoded)-1])
	if err == nil {
		t.Fatal("expected truncated batch to be rejected")
	}
}

func TestDecodeBatchRejectsTrailingBytes(t *testing.T) {
	encoded, err := EncodeBatch(MessageBatch{
		Messages: []Message{
			{Topic: "events", Value: []byte("hello")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	encoded = append(encoded, 0xFF)

	_, err = DecodeBatch(encoded)
	if err == nil {
		t.Fatal("expected trailing bytes to be rejected")
	}
}

func TestDecodeBatchRejectsExcessiveMessageCount(t *testing.T) {
	data := make([]byte, batchHeaderSize)
	binary.BigEndian.PutUint32(data, MaxBatchMessages+1)

	_, err := DecodeBatch(data)
	if err == nil {
		t.Fatal("expected excessive message count to be rejected")
	}
}
