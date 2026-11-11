package broker

import (
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func TestProduceHandlerRejectsMalformedRequest(t *testing.T) {
	handler := NewProduceHandler()
	req := protocol.Request{APIKey: protocol.APIPRODUCE, APIVersion: 1, CorrelationID: 1, Payload: []byte("test")}
	if _, err := handler.Handle(req); err == nil {
		t.Fatal("expected malformed produce payload to fail")
	}
}

func TestConsumeHandler(t *testing.T) {
	registry := topic.NewRegistry()
	testTopic, err := registry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := testTopic.OpenLogs(t.TempDir(), 1024); err != nil {
		t.Fatal(err)
	}
	defer testTopic.CloseLogs()
	partition, _ := testTopic.Partition(0)
	if _, err := partition.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	payload, err := protocol.EncodeConsumeRequest(protocol.ConsumeRequest{Topic: "orders", PartitionID: 0, Offset: 0, MaxMessages: 1})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewConsumeHandler(registry)
	responsePayload, err := handler.Handle(protocol.Request{APIKey: protocol.APICONSUME, APIVersion: ConsumeAPIVersion, CorrelationID: 2, Payload: payload})
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	response, err := protocol.DecodeConsumeResponse(responsePayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Records) != 1 || string(response.Records[0].Value) != "hello" {
		t.Fatalf("unexpected response: %+v", response)
	}
}
