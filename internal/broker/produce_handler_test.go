package broker

import (
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/recordmeta"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func TestProduceHandlerAppendsAndReturnsOffsets(t *testing.T) {
	registry := topic.NewRegistry()
	orders, err := registry.CreateWithPartitionCount("orders", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := orders.OpenLogs(t.TempDir(), 1024*1024); err != nil {
		t.Fatal(err)
	}
	defer orders.CloseLogs()

	payload, err := protocol.EncodeProduceRequest(protocol.ProduceRequest{
		AckMode: protocol.AckLeader,
		Batch: protocol.MessageBatch{Messages: []protocol.Message{
			{Topic: "orders", Key: []byte("customer-1"), Value: []byte("created"), Timestamp: 100},
			{Topic: "orders", Key: []byte("customer-1"), Value: []byte("paid"), Timestamp: 101},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewProduceHandler(registry)
	responsePayload, err := handler.Handle(protocol.Request{APIKey: protocol.APIPRODUCE, APIVersion: ProduceAPIVersion, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.DecodeProduceResponse(responsePayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Records) != 2 {
		t.Fatalf("got %d results, want 2", len(response.Records))
	}
	if response.Records[0].PartitionID != response.Records[1].PartitionID {
		t.Fatal("same key was routed to different partitions")
	}
	if response.Records[0].Offset != 0 || response.Records[1].Offset != 1 {
		t.Fatalf("unexpected offsets: %+v", response.Records)
	}
	partition, err := orders.Partition(response.Records[0].PartitionID)
	if err != nil {
		t.Fatal(err)
	}
	first, timestamp, err := partition.ReadWithTimestamp(0)
	if err != nil {
		t.Fatal(err)
	}
	key, value, isEnvelope, err := recordmeta.DecodeMessage(first)
	if err != nil {
		t.Fatal(err)
	}
	if !isEnvelope || string(key) != "customer-1" || string(value) != "created" || timestamp != 100 {
		t.Fatalf("stored record key=%q value=%q timestamp=%d envelope=%t", key, value, timestamp, isEnvelope)
	}
}

func TestProduceHandlerValidatesBatchBeforeAppending(t *testing.T) {
	registry := topic.NewRegistry()
	orders, err := registry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := orders.OpenLogs(t.TempDir(), 1024*1024); err != nil {
		t.Fatal(err)
	}
	defer orders.CloseLogs()

	payload, err := protocol.EncodeProduceRequest(protocol.ProduceRequest{
		AckMode: protocol.AckLeader,
		Batch: protocol.MessageBatch{Messages: []protocol.Message{
			{Topic: "orders", Value: []byte("would-have-been-written")},
			{Topic: "missing", Value: []byte("bad-topic")},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewProduceHandler(registry).Handle(protocol.Request{APIKey: protocol.APIPRODUCE, APIVersion: 1, Payload: payload})
	if err == nil {
		t.Fatal("expected unknown topic error")
	}
	partition, _ := orders.Partition(0)
	next, err := partition.NextOffset()
	if err != nil {
		t.Fatal(err)
	}
	if next != 0 {
		t.Fatalf("first message was appended before validation completed; next=%d", next)
	}
}

func TestProduceHandlerRequiresSupportedVersion(t *testing.T) {
	_, err := NewProduceHandler().Handle(protocol.Request{APIKey: protocol.APIPRODUCE, APIVersion: 99})
	if err == nil {
		t.Fatal("expected unsupported version")
	}
}
