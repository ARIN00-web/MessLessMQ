package broker

import (
	"errors"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/consumer"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func setupConsumeHandler(t *testing.T) (*topic.Registry, *ConsumeHandler) {
	t.Helper()
	registry := topic.NewRegistry()
	testTopic, err := registry.CreateWithPartitionCount("orders", 2)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}
	if err := testTopic.OpenLogs(t.TempDir(), 1024*1024); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}
	t.Cleanup(func() {
		if err := testTopic.CloseLogs(); err != nil {
			t.Errorf("CloseLogs() error: %v", err)
		}
	})
	partition, err := testTopic.Partition(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"first", "second", "third"} {
		if _, err := partition.Append([]byte(value)); err != nil {
			t.Fatalf("Append() error: %v", err)
		}
	}
	return registry, NewConsumeHandler(registry)
}

func makeConsumeRequest(t *testing.T, topicName string, partitionID uint32, offset uint64, max uint32) protocol.Request {
	t.Helper()
	payload, err := protocol.EncodeConsumeRequest(protocol.ConsumeRequest{
		Topic: topicName, PartitionID: partitionID, Offset: offset, MaxMessages: max,
	})
	if err != nil {
		t.Fatalf("EncodeConsumeRequest() error: %v", err)
	}
	return protocol.Request{APIKey: protocol.APICONSUME, APIVersion: ConsumeAPIVersion, CorrelationID: 1, Payload: payload}
}

func TestConsumeHandlerFetchesRecords(t *testing.T) {
	_, handler := setupConsumeHandler(t)
	req := makeConsumeRequest(t, "orders", 0, 1, 2)

	payload, err := handler.Handle(req)
	if err != nil {
		t.Fatalf("Handle() error: %v", err)
	}
	response, err := protocol.DecodeConsumeResponse(payload)
	if err != nil {
		t.Fatalf("DecodeConsumeResponse() error: %v", err)
	}
	if response.StartOffset != 0 || response.LogEndOffset != 3 || response.NextOffset != 3 {
		t.Fatalf("response offsets = %+v", response)
	}
	if len(response.Records) != 2 {
		t.Fatalf("record count = %d, want 2", len(response.Records))
	}
	if string(response.Records[0].Value) != "second" || string(response.Records[1].Value) != "third" {
		t.Fatalf("records = %+v", response.Records)
	}
}

func TestConsumeHandlerReturnsEmptyFetchAtLogEnd(t *testing.T) {
	_, handler := setupConsumeHandler(t)
	payload, err := handler.Handle(makeConsumeRequest(t, "orders", 0, 3, 10))
	if err != nil {
		t.Fatalf("Handle() error: %v", err)
	}
	response, err := protocol.DecodeConsumeResponse(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Records) != 0 || response.NextOffset != 3 {
		t.Fatalf("empty response = %+v", response)
	}
}

func TestConsumeHandlerRejectsUnknownTopic(t *testing.T) {
	_, handler := setupConsumeHandler(t)
	_, err := handler.Handle(makeConsumeRequest(t, "payments", 0, 0, 10))
	if !errors.Is(err, topic.ErrTopicNotFound) {
		t.Fatalf("Handle() error = %v, want ErrTopicNotFound", err)
	}
}

func TestConsumeHandlerRejectsUnknownPartition(t *testing.T) {
	_, handler := setupConsumeHandler(t)
	_, err := handler.Handle(makeConsumeRequest(t, "orders", 9, 0, 10))
	if !errors.Is(err, topic.ErrPartitionNotFound) {
		t.Fatalf("Handle() error = %v, want ErrPartitionNotFound", err)
	}
}

func TestConsumeHandlerRejectsOffsetBeyondLogEnd(t *testing.T) {
	_, handler := setupConsumeHandler(t)
	_, err := handler.Handle(makeConsumeRequest(t, "orders", 0, 99, 10))
	if !errors.Is(err, consumer.ErrOffsetOutOfRange) {
		t.Fatalf("Handle() error = %v, want ErrOffsetOutOfRange", err)
	}
}

func TestConsumeHandlerRejectsUnsupportedVersion(t *testing.T) {
	_, handler := setupConsumeHandler(t)
	req := makeConsumeRequest(t, "orders", 0, 0, 10)
	req.APIVersion = 99
	_, err := handler.Handle(req)
	if !errors.Is(err, protocol.ErrUnsupportedVersion) {
		t.Fatalf("Handle() error = %v, want ErrUnsupportedVersion", err)
	}
}

func TestConsumeHandlerRejectsMalformedPayload(t *testing.T) {
	_, handler := setupConsumeHandler(t)
	_, err := handler.Handle(protocol.Request{APIKey: protocol.APICONSUME, APIVersion: 1, Payload: []byte{1, 2}})
	if !errors.Is(err, protocol.ErrInvalidRequest) {
		t.Fatalf("Handle() error = %v, want ErrInvalidRequest", err)
	}
}
