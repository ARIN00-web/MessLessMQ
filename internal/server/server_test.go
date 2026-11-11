package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/config"
	"github.com/ARIN00-web/messlessmq/internal/network"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func testConfig(dataDir string) config.Config {
	cfg := config.Defaults()
	cfg.BrokerID = "test-broker"
	cfg.ListenAddress = "127.0.0.1:9092"
	cfg.AdvertiseAddress = "127.0.0.1:9092"
	cfg.DataDir = dataDir
	cfg.RequestTimeout = 2 * time.Second
	cfg.ShutdownTimeout = 2 * time.Second
	cfg.RateLimitPerSecond = 1000
	cfg.RateLimitBurst = 1000
	return cfg
}

func startTestServer(t *testing.T, runtime *Server) (net.Conn, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Serve(ctx, listener) }()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn, cancel, done
}

func exchange(t *testing.T, conn net.Conn, request protocol.Request) protocol.Response {
	t.Helper()
	frame, err := protocol.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := network.WriteFrame(conn, frame); err != nil {
		t.Fatal(err)
	}
	responseFrame, err := network.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.DecodeResponse(responseFrame)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func stopTestServer(t *testing.T, conn net.Conn, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	_ = conn.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("server did not stop")
	}
}

func TestRuntimeProducesConsumesAndPersistsAcrossRestart(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "broker-data")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	first, err := New(Options{Config: testConfig(dataDir), BootstrapTopics: []TopicDefinition{{Name: "orders", Partitions: 3}}, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	conn, cancel, done := startTestServer(t, first)

	producePayload, err := protocol.EncodeProduceRequest(protocol.ProduceRequest{
		AckMode: protocol.AckLeader,
		Batch: protocol.MessageBatch{Messages: []protocol.Message{
			{Topic: "orders", Key: []byte("customer-7"), Value: []byte("order-created"), Timestamp: 12345},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	produceResponse := exchange(t, conn, protocol.Request{APIKey: protocol.APIPRODUCE, APIVersion: 1, CorrelationID: 1, Payload: producePayload})
	if produceResponse.ErrorCode != protocol.ErrorNone {
		t.Fatalf("produce error code=%d payload=%q", produceResponse.ErrorCode, produceResponse.Payload)
	}
	appendResult, err := protocol.DecodeProduceResponse(produceResponse.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(appendResult.Records) != 1 || appendResult.Records[0].Offset != 0 {
		t.Fatalf("unexpected produce response: %+v", appendResult)
	}

	metadataPayload, err := protocol.EncodeMetadataRequest(protocol.MetadataRequest{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	metadataResponse := exchange(t, conn, protocol.Request{APIKey: protocol.APIMETADATA, APIVersion: protocol.MetadataAPIVersion, CorrelationID: 2, Payload: metadataPayload})
	if metadataResponse.ErrorCode != protocol.ErrorNone {
		t.Fatalf("metadata error code=%d", metadataResponse.ErrorCode)
	}
	metadata, err := protocol.DecodeMetadataResponse(metadataResponse.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.Brokers) != 1 || metadata.Brokers[0].ID != "test-broker" || len(metadata.Partitions) != 3 {
		t.Fatalf("unexpected local metadata: %+v", metadata)
	}

	consumePayload, err := protocol.EncodeConsumeRequest(protocol.ConsumeRequest{Topic: "orders", PartitionID: appendResult.Records[0].PartitionID, Offset: 0, MaxMessages: 10})
	if err != nil {
		t.Fatal(err)
	}
	consumeResponse := exchange(t, conn, protocol.Request{APIKey: protocol.APICONSUME, APIVersion: 2, CorrelationID: 3, Payload: consumePayload})
	if consumeResponse.ErrorCode != protocol.ErrorNone {
		t.Fatalf("consume error code=%d", consumeResponse.ErrorCode)
	}
	consumed, err := protocol.DecodeConsumeResponseV2(consumeResponse.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(consumed.Records) != 1 || string(consumed.Records[0].Value) != "order-created" || string(consumed.Records[0].Key) != "customer-7" || consumed.Records[0].Timestamp != 12345 {
		t.Fatalf("unexpected consumed records: %+v", consumed.Records)
	}
	stopTestServer(t, conn, cancel, done)

	// Reconstruct the runtime without bootstrap definitions. The catalog and
	// partition data must be recovered from the same data directory.
	restarted, err := New(Options{Config: testConfig(dataDir), Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	conn2, cancel2, done2 := startTestServer(t, restarted)
	consumePayload, err = protocol.EncodeConsumeRequest(protocol.ConsumeRequest{Topic: "orders", PartitionID: appendResult.Records[0].PartitionID, Offset: 0, MaxMessages: 10})
	if err != nil {
		t.Fatal(err)
	}
	response2 := exchange(t, conn2, protocol.Request{APIKey: protocol.APICONSUME, APIVersion: 2, CorrelationID: 4, Payload: consumePayload})
	if response2.ErrorCode != protocol.ErrorNone {
		t.Fatalf("consume after restart error code=%d", response2.ErrorCode)
	}
	recovered, err := protocol.DecodeConsumeResponseV2(response2.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered.Records) != 1 || string(recovered.Records[0].Value) != "order-created" || string(recovered.Records[0].Key) != "customer-7" {
		t.Fatalf("message did not survive restart: %+v", recovered.Records)
	}
	stopTestServer(t, conn2, cancel2, done2)
}

func TestRuntimeRejectsPartitionCountChangeForPersistedTopic(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "broker-data")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runtime, err := New(Options{Config: testConfig(dataDir), BootstrapTopics: []TopicDefinition{{Name: "orders", Partitions: 2}}, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	if err := closeTopics(runtime.topics); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Config: testConfig(dataDir), BootstrapTopics: []TopicDefinition{{Name: "orders", Partitions: 3}}, Logger: logger}); err == nil {
		t.Fatal("expected persisted topic partition count mismatch")
	}
}

func TestParseBootstrapTopics(t *testing.T) {
	definitions, err := ParseBootstrapTopics("orders:3, payments:1")
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 2 || definitions[0].Name != "orders" || definitions[0].Partitions != 3 || definitions[1].Name != "payments" || definitions[1].Partitions != 1 {
		t.Fatalf("unexpected definitions: %+v", definitions)
	}
	if _, err := ParseBootstrapTopics("orders:0"); err == nil {
		t.Fatal("expected zero partitions to fail")
	}
	if _, err := ParseBootstrapTopics("orders:1,orders:2"); err == nil {
		t.Fatal("expected duplicate topic to fail")
	}
}
