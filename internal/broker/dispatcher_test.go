package broker

import (
	"strings"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func TestDispatcherProduce(t *testing.T) {
	dispatcher := NewDispatcher()

	req := protocol.Request{
		APIKey:        protocol.APIPRODUCE,
		APIVersion:    1,
		CorrelationID: 1,
		Payload:       []byte("test"),
	}

	response, err := dispatcher.Dispatch(req)
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
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

func TestDispatcherConsume(t *testing.T) {
	dispatcher := NewDispatcher()

	req := protocol.Request{
		APIKey:        protocol.APICONSUME,
		APIVersion:    1,
		CorrelationID: 2,
		Payload:       []byte("test"),
	}

	response, err := dispatcher.Dispatch(req)
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
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

func TestDispatcherUnknownAPI(t *testing.T) {
	dispatcher := NewDispatcher()

	req := protocol.Request{
		APIKey:        999,
		APIVersion:    1,
		CorrelationID: 3,
		Payload:       []byte("test"),
	}

	_, err := dispatcher.Dispatch(req)

	if err == nil {
		t.Fatal("expected error for unknown API key, got nil")
	}

	if !strings.Contains(err.Error(), "unknown API key") {
		t.Fatalf(
			"unexpected error: %v",
			err,
		)
	}
}
