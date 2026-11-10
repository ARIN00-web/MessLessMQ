package interbroker

import (
	"errors"
	"testing"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	want := Envelope{Type: MessageReplicationBatch, RequestID: 42, SourceID: "broker-a", TargetID: "broker-b", Payload: []byte{0, 1, 2, 3}}
	encoded, err := EncodeEnvelope(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != ProtocolVersion || got.Type != want.Type || got.RequestID != want.RequestID || got.SourceID != want.SourceID || got.TargetID != want.TargetID || string(got.Payload) != string(want.Payload) {
		t.Fatalf("decoded envelope = %+v, want %+v", got, want)
	}
}

func TestEnvelopeRejectsMalformedAndOversizedInput(t *testing.T) {
	if _, err := DecodeEnvelope([]byte{1}); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("short envelope error = %v", err)
	}
	if _, err := EncodeEnvelope(Envelope{Type: MessageHeartbeat, RequestID: 1, SourceID: "a", TargetID: "b", Payload: make([]byte, MaxPeerPayload+1)}); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("oversized payload error = %v", err)
	}
	encoded, err := EncodeEnvelope(Envelope{Type: MessageHeartbeat, RequestID: 1, SourceID: "a", TargetID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	encoded[12] = 2
	if _, err := DecodeEnvelope(encoded); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("invalid flags error = %v", err)
	}
}
