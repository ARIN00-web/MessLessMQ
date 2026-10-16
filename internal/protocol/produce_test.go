package protocol

import "testing"

func TestProduceRequestRoundTrip(t *testing.T) {
	original := ProduceRequest{
		AckMode: AckLeader,
		Batch: MessageBatch{
			Messages: []Message{
				{
					Topic:     "orders",
					Key:       []byte("order-1"),
					Value:     []byte("created"),
					Timestamp: 1000,
				},
			},
		},
	}

	encoded, err := EncodeProduceRequest(original)
	if err != nil {
		t.Fatalf("EncodeProduceRequest failed: %v", err)
	}

	decoded, err := DecodeProduceRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeProduceRequest failed: %v", err)
	}

	if decoded.AckMode != original.AckMode {
		t.Fatalf("ack mode: got %v, want %v",
			decoded.AckMode, original.AckMode)
	}

	if len(decoded.Batch.Messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(decoded.Batch.Messages))
	}

	if decoded.Batch.Messages[0].Topic != "orders" {
		t.Fatalf("unexpected topic: %q", decoded.Batch.Messages[0].Topic)
	}
}

func TestAckModeValidation(t *testing.T) {
	valid := []AckMode{AckNone, AckLeader, AckAll}

	for _, mode := range valid {
		if err := mode.Validate(); err != nil {
			t.Errorf("mode %d should be valid: %v", mode, err)
		}
	}

	if err := AckMode(2).Validate(); err == nil {
		t.Fatal("expected unsupported acknowledgement mode to fail")
	}
}

func TestDecodeProduceRequestRejectsInvalidAckMode(t *testing.T) {
	data := make([]byte, producePayloadHeaderSize)
	data[1] = 2 // AckMode 2 is unsupported.

	if _, err := DecodeProduceRequest(data); err == nil {
		t.Fatal("expected invalid acknowledgement mode to fail")
	}
}

func TestDecodeProduceRequestRejectsTruncatedBatch(t *testing.T) {
	encoded, err := EncodeProduceRequest(ProduceRequest{
		AckMode: AckLeader,
		Batch: MessageBatch{
			Messages: []Message{
				{Topic: "events", Value: []byte("hello")},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Remove a byte while leaving the declared batch length unchanged.
	_, err = DecodeProduceRequest(encoded[:len(encoded)-1])
	if err == nil {
		t.Fatal("expected truncated produce request to fail")
	}
}
