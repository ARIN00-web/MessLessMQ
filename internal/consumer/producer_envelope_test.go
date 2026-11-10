package consumer

import (
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/recordmeta"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func TestConsumerUnwrapsIdempotentProducerEnvelope(t *testing.T) {
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
	encoded, err := recordmeta.Encode(recordmeta.ProducerMetadata{
		ProducerID: "orders-api", Epoch: 3, Sequence: 7,
	}, []byte("order-created"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append(encoded); err != nil {
		t.Fatal(err)
	}

	reader, err := New(partition)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Fetch("orders", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 {
		t.Fatalf("record count = %d, want 1", len(result.Records))
	}
	record := result.Records[0]
	if string(record.Value) != "order-created" {
		t.Fatalf("consumer exposed envelope instead of payload: %q", record.Value)
	}
	if record.ProducerID != "orders-api" || record.ProducerEpoch != 3 || record.ProducerSequence != 7 {
		t.Fatalf("producer metadata = %+v", record)
	}
}
