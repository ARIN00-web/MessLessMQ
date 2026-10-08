package broker

import (
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func TestProduceHandler(t *testing.T) {
	handler := NewProduceHandler()

	req := protocol.Request{
		APIKey:        protocol.APIPRODUCE,
		APIVersion:    1,
		CorrelationID: 1,
		Payload:       []byte("test"),
	}

	response, err := handler.Handle(req)
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	expected := "PRODUCE handler not implemented"

	if string(response) != expected {
		t.Fatalf(
			"unexpected response: got %q, want %q",
			response,
			expected,
		)
	}
}

func TestConsumeHandler(t *testing.T) {
	handler := NewConsumeHandler()

	req := protocol.Request{
		APIKey:        protocol.APICONSUME,
		APIVersion:    1,
		CorrelationID: 2,
		Payload:       []byte("test"),
	}

	response, err := handler.Handle(req)
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	expected := "CONSUME handler not implemented"

	if string(response) != expected {
		t.Fatalf(
			"unexpected response: got %q, want %q",
			response,
			expected,
		)
	}
}