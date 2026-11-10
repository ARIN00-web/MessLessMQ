package broker

import (
	"strings"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func TestDispatcherProduce(t *testing.T) {
	dispatcher := NewDispatcher()
	req := protocol.Request{APIKey: protocol.APIPRODUCE, APIVersion: 1, CorrelationID: 1, Payload: []byte("test")}
	response, err := dispatcher.Dispatch(req)
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	if string(response) != "PRODUCE handler not implemented" {
		t.Fatalf("unexpected response: %q", response)
	}
}

func TestDispatcherConsume(t *testing.T) {
	registry := topic.NewRegistry()
	testTopic, err := registry.CreateWithPartitionCount("orders", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := testTopic.OpenLogs(t.TempDir(), 1024); err != nil {
		t.Fatal(err)
	}
	defer testTopic.CloseLogs()
	partition, err := testTopic.Partition(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append([]byte("order-created")); err != nil {
		t.Fatal(err)
	}

	payload, err := protocol.EncodeConsumeRequest(protocol.ConsumeRequest{Topic: "orders", PartitionID: 1, Offset: 0, MaxMessages: 10})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := NewDispatcher(registry)
	responsePayload, err := dispatcher.Dispatch(protocol.Request{APIKey: protocol.APICONSUME, APIVersion: 1, CorrelationID: 2, Payload: payload})
	if err != nil {
		t.Fatalf("Dispatch consume failed: %v", err)
	}
	response, err := protocol.DecodeConsumeResponse(responsePayload)
	if err != nil {
		t.Fatalf("DecodeConsumeResponse() error: %v", err)
	}
	if len(response.Records) != 1 || string(response.Records[0].Value) != "order-created" {
		t.Fatalf("unexpected consume response: %+v", response)
	}
}

func TestDispatcherUnknownAPI(t *testing.T) {
	dispatcher := NewDispatcher()
	req := protocol.Request{APIKey: 999, APIVersion: 1, CorrelationID: 3, Payload: []byte("test")}
	_, err := dispatcher.Dispatch(req)
	if err == nil {
		t.Fatal("expected error for unknown API key, got nil")
	}
	if !strings.Contains(err.Error(), "unknown API key") {
		t.Fatalf("unexpected error: %v", err)
	}
}
