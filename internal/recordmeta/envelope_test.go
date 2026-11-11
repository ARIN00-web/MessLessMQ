package recordmeta

import (
	"errors"
	"testing"
)

func TestProducerEnvelopeRoundTrip(t *testing.T) {
	meta := ProducerMetadata{ProducerID: "orders-api", Epoch: 7, Sequence: 42}
	payload := []byte("order-created")
	encoded, err := Encode(meta, payload)
	if err != nil {
		t.Fatal(err)
	}
	gotMeta, gotPayload, isEnvelope, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !isEnvelope {
		t.Fatal("expected producer envelope")
	}
	if gotMeta != meta {
		t.Fatalf("metadata = %+v, want %+v", gotMeta, meta)
	}
	if string(gotPayload) != string(payload) {
		t.Fatalf("payload = %q, want %q", gotPayload, payload)
	}
}

func TestDecodePlainPayload(t *testing.T) {
	payload := []byte("ordinary message")
	_, got, isEnvelope, err := Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if isEnvelope {
		t.Fatal("plain payload was classified as envelope")
	}
	if string(got) != string(payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestDecodeRejectsCorruptEnvelope(t *testing.T) {
	encoded, err := Encode(ProducerMetadata{ProducerID: "orders-api"}, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 0xFF
	_, _, isEnvelope, err := Decode(encoded)
	if !isEnvelope || !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("Decode() = isEnvelope %t, err %v", isEnvelope, err)
	}
}

func TestEncodeRejectsInvalidProducerID(t *testing.T) {
	_, err := Encode(ProducerMetadata{ProducerID: "invalid producer"}, []byte("x"))
	if !errors.Is(err, ErrInvalidProducerID) {
		t.Fatalf("error = %v, want ErrInvalidProducerID", err)
	}
}

func TestEncodeRejectsOversizedPayload(t *testing.T) {
	_, err := Encode(ProducerMetadata{ProducerID: "p"}, make([]byte, MaxPayloadSize+1))
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestStoredMessageRoundTripPreservesNilAndEmptyKeys(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
	}{{name: "nil key", key: nil}, {name: "empty key", key: []byte{}}, {name: "normal key", key: []byte("account-7")}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeMessage(tc.key, []byte("payload"))
			if err != nil {
				t.Fatal(err)
			}
			key, value, isEnvelope, err := DecodeMessage(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !isEnvelope || string(value) != "payload" {
				t.Fatalf("decode got envelope=%t value=%q", isEnvelope, value)
			}
			if (key == nil) != (tc.key == nil) || string(key) != string(tc.key) {
				t.Fatalf("key=%v want=%v", key, tc.key)
			}
		})
	}
}

func TestStoredMessageRejectsChecksumCorruption(t *testing.T) {
	encoded, err := EncodeMessage([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 1
	_, _, isEnvelope, err := DecodeMessage(encoded)
	if !isEnvelope || !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("got envelope=%t err=%v", isEnvelope, err)
	}
}
