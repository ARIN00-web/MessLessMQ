package protocol

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestProduceRequestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		mode AckMode
	}{
		{name: "no acknowledgement", mode: AckNone},
		{name: "leader acknowledgement", mode: AckLeader},
		{name: "all acknowledgement", mode: AckAll},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := ProduceRequest{
				AckMode: tt.mode,
				Batch: MessageBatch{
					Messages: []Message{
						{
							Topic:     "orders",
							Key:       []byte("order-123"),
							Value:     []byte("created"),
							Timestamp: 1000,
						},
						{
							Topic:     "orders",
							Key:       nil,
							Value:     []byte("updated"),
							Timestamp: 2000,
						},
					},
				},
			}

			encoded, err := EncodeProduceRequest(original)
			if err != nil {
				t.Fatalf("EncodeProduceRequest() error: %v", err)
			}

			decoded, err := DecodeProduceRequest(encoded)
			if err != nil {
				t.Fatalf("DecodeProduceRequest() error: %v", err)
			}

			if decoded.AckMode != original.AckMode {
				t.Errorf("AckMode = %v, want %v", decoded.AckMode, original.AckMode)
			}

			if len(decoded.Batch.Messages) != len(original.Batch.Messages) {
				t.Fatalf(
					"got %d messages, want %d",
					len(decoded.Batch.Messages),
					len(original.Batch.Messages),
				)
			}

			for i, want := range original.Batch.Messages {
				got := decoded.Batch.Messages[i]

				if got.Topic != want.Topic {
					t.Errorf("message %d topic = %q, want %q", i, got.Topic, want.Topic)
				}
				if string(got.Key) != string(want.Key) {
					t.Errorf("message %d key = %q, want %q", i, got.Key, want.Key)
				}
				if (got.Key == nil) != (want.Key == nil) {
					t.Errorf("message %d key nil status differs", i)
				}
				if string(got.Value) != string(want.Value) {
					t.Errorf("message %d value = %q, want %q", i, got.Value, want.Value)
				}
				if got.Timestamp != want.Timestamp {
					t.Errorf(
						"message %d timestamp = %d, want %d",
						i,
						got.Timestamp,
						want.Timestamp,
					)
				}
			}
		})
	}
}

func TestProduceRequestRejectsInvalidAckMode(t *testing.T) {
	payload := make([]byte, producePayloadHeaderSize)

	binary.BigEndian.PutUint16(payload[0:2], 2)
	binary.BigEndian.PutUint32(payload[2:6], 0)

	_, err := DecodeProduceRequest(payload)
	if !errors.Is(err, ErrInvalidAckMode) {
		t.Fatalf("expected ErrInvalidAckMode, got %v", err)
	}
}

func TestProduceRequestRejectsTruncatedPayload(t *testing.T) {
	payload := make([]byte, producePayloadHeaderSize-1)

	_, err := DecodeProduceRequest(payload)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestProduceRequestRejectsInvalidBatchLength(t *testing.T) {
	payload := make([]byte, producePayloadHeaderSize)

	binary.BigEndian.PutUint16(payload[0:2], uint16(int16(AckLeader)))
	binary.BigEndian.PutUint32(payload[2:6], 100)

	_, err := DecodeProduceRequest(payload)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestProduceRequestRejectsOversizedBatch(t *testing.T) {
	payload := make([]byte, producePayloadHeaderSize)

	binary.BigEndian.PutUint16(payload[0:2], uint16(int16(AckLeader)))
	binary.BigEndian.PutUint32(payload[2:6], uint32(MaxBatchSize+1))

	_, err := DecodeProduceRequest(payload)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("expected ErrMessageTooLarge, got %v", err)
	}
}

func TestProduceRequestRejectsMalformedBatch(t *testing.T) {
	// A batch with a count of zero is structurally invalid.
	batch := make([]byte, 4)
	binary.BigEndian.PutUint32(batch, 0)

	payload := make([]byte, producePayloadHeaderSize+len(batch))
	binary.BigEndian.PutUint16(payload[0:2], uint16(int16(AckLeader)))
	binary.BigEndian.PutUint32(payload[2:6], uint32(len(batch)))
	copy(payload[producePayloadHeaderSize:], batch)

	_, err := DecodeProduceRequest(payload)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestProduceRequestRejectsTrailingOrMismatchedData(t *testing.T) {
	valid := ProduceRequest{
		AckMode: AckLeader,
		Batch: MessageBatch{
			Messages: []Message{
				{
					Topic: "orders",
					Value: []byte("created"),
				},
			},
		},
	}

	payload, err := EncodeProduceRequest(valid)
	if err != nil {
		t.Fatalf("EncodeProduceRequest() error: %v", err)
	}

	// Add an extra byte without updating the declared batch length.
	payload = append(payload, 0xFF)

	_, err = DecodeProduceRequest(payload)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestProduceRequestRejectsEmptyBatchOnEncode(t *testing.T) {
	req := ProduceRequest{
		AckMode: AckLeader,
		Batch:   MessageBatch{},
	}

	_, err := EncodeProduceRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestProduceRequestRejectsInvalidAckModeOnEncode(t *testing.T) {
	req := ProduceRequest{
		AckMode: AckMode(2),
		Batch: MessageBatch{
			Messages: []Message{
				{Topic: "orders", Value: []byte("created")},
			},
		},
	}

	_, err := EncodeProduceRequest(req)
	if !errors.Is(err, ErrInvalidAckMode) {
		t.Fatalf("expected ErrInvalidAckMode, got %v", err)
	}
}
