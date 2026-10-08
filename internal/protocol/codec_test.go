package protocol

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRequest(t *testing.T) {
	original := Request{
		APIKey:        APIPRODUCE,
		APIVersion:    1,
		CorrelationID: 42,
		Payload:       []byte("hello MessLessMQ"),
	}

	encoded, err := EncodeRequest(original)
	if err != nil {
		t.Fatalf("EncodeRequest failed: %v", err)
	}

	decoded, err := DecodeRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeRequest failed: %v", err)
	}

	if decoded.APIKey != original.APIKey {
		t.Errorf(
			"API key mismatch: got %d, want %d",
			decoded.APIKey,
			original.APIKey,
		)
	}

	if decoded.APIVersion != original.APIVersion {
		t.Errorf(
			"API version mismatch: got %d, want %d",
			decoded.APIVersion,
			original.APIVersion,
		)
	}

	if decoded.CorrelationID != original.CorrelationID {
		t.Errorf(
			"correlation ID mismatch: got %d, want %d",
			decoded.CorrelationID,
			original.CorrelationID,
		)
	}

	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Errorf(
			"payload mismatch: got %q, want %q",
			decoded.Payload,
			original.Payload,
		)
	}
}

func TestDecodeRequestTooShort(t *testing.T) {
	data := []byte{0x00, 0x01, 0x00}

	_, err := DecodeRequest(data)

	if err == nil {
		t.Fatal("expected error for short request, got nil")
	}
}

func TestEncodeRequestInvalidAPIKey(t *testing.T) {
	req := Request{
		APIKey:        0,
		APIVersion:    1,
		CorrelationID: 1,
		Payload:       []byte("test"),
	}

	_, err := EncodeRequest(req)

	if err == nil {
		t.Fatal("expected error for invalid API key, got nil")
	}
}

func TestEncodeDecodeResponse(t *testing.T) {
	original := Response{
		APIKey:        APIPRODUCE,
		APIVersion:    1,
		CorrelationID: 42,
		ErrorCode:     ErrorNone,
		Payload:       []byte("produce successful"),
	}

	encoded, err := EncodeResponse(original)
	if err != nil {
		t.Fatalf("EncodeResponse failed: %v", err)
	}

	decoded, err := DecodeResponse(encoded)
	if err != nil {
		t.Fatalf("DecodeResponse failed: %v", err)
	}

	if decoded.APIKey != original.APIKey {
		t.Errorf(
			"API key mismatch: got %d, want %d",
			decoded.APIKey,
			original.APIKey,
		)
	}

	if decoded.APIVersion != original.APIVersion {
		t.Errorf(
			"API version mismatch: got %d, want %d",
			decoded.APIVersion,
			original.APIVersion,
		)
	}

	if decoded.CorrelationID != original.CorrelationID {
		t.Errorf(
			"correlation ID mismatch: got %d, want %d",
			decoded.CorrelationID,
			original.CorrelationID,
		)
	}

	if decoded.ErrorCode != original.ErrorCode {
		t.Errorf(
			"error code mismatch: got %d, want %d",
			decoded.ErrorCode,
			original.ErrorCode,
		)
	}

	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Errorf(
			"payload mismatch: got %q, want %q",
			decoded.Payload,
			original.Payload,
		)
	}
}

func TestDecodeResponseTooShort(t *testing.T) {
	data := []byte{
		0x00,
		0x01,
		0x00,
	}

	_, err := DecodeResponse(data)

	if err == nil {
		t.Fatal("expected error for short response, got nil")
	}
}

func TestEncodeResponseInvalidAPIKey(t *testing.T) {
	response := Response{
		APIKey:        0,
		APIVersion:    1,
		CorrelationID: 1,
		ErrorCode:     ErrorNone,
		Payload:       []byte("test"),
	}

	_, err := EncodeResponse(response)

	if err == nil {
		t.Fatal("expected error for invalid API key, got nil")
	}
}
