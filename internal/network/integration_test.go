package network

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func startIntegrationServer(
	t *testing.T,
	dispatcher *broker.Dispatcher,
) (net.Conn, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start test listener: %v", err)
	}
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		serverConn, err := listener.Accept()
		if err != nil {
			_ = listener.Close()
			return
		}
		_ = listener.Close()
		HandleConnection(serverConn, dispatcher, make(chan struct{}))
	}()
	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatalf("failed to connect to test server: %v", err)
	}
	if err := clientConn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		_ = clientConn.Close()
		t.Fatalf("failed to set client deadline: %v", err)
	}
	return clientConn, serverDone
}

func sendTestRequest(t *testing.T, conn net.Conn, req protocol.Request) protocol.Response {
	t.Helper()
	requestFrame, err := protocol.EncodeRequest(req)
	if err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}
	if err := WriteFrame(conn, requestFrame); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}
	responseFrame, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	response, err := protocol.DecodeResponse(responseFrame)
	if err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	return response
}

func TestConnectionIntegration(t *testing.T) {
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
	if _, err := partition.AppendAt([]byte("order-created"), 1234); err != nil {
		t.Fatal(err)
	}

	conn, serverDone := startIntegrationServer(t, broker.NewDispatcher(registry))
	defer conn.Close()

	// First request: an unknown API should return an error without closing
	// the connection.
	first := sendTestRequest(t, conn, protocol.Request{
		APIKey: 999, APIVersion: 1, CorrelationID: 101,
		Payload: []byte("unknown API"),
	})
	if first.CorrelationID != 101 || first.ErrorCode != protocol.ErrorUnknownAPI {
		t.Fatalf("unexpected first response: %+v", first)
	}

	consumePayload, err := protocol.EncodeConsumeRequest(protocol.ConsumeRequest{
		Topic: "orders", PartitionID: 1, Offset: 0, MaxMessages: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	second := sendTestRequest(t, conn, protocol.Request{
		APIKey: protocol.APICONSUME, APIVersion: 1, CorrelationID: 102,
		Payload: consumePayload,
	})
	if second.CorrelationID != 102 || second.ErrorCode != protocol.ErrorNone {
		t.Fatalf("unexpected second response: %+v", second)
	}
	consumeResponse, err := protocol.DecodeConsumeResponse(second.Payload)
	if err != nil {
		t.Fatalf("DecodeConsumeResponse() error: %v", err)
	}
	if len(consumeResponse.Records) != 1 || string(consumeResponse.Records[0].Value) != "order-created" || consumeResponse.Records[0].Timestamp != 1234 {
		t.Fatalf("unexpected consume response: %+v", consumeResponse)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close client connection: %v", err)
	}
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not exit after client disconnect")
	}
}

func TestConnectionReturnsUnknownTopicError(t *testing.T) {
	conn, serverDone := startIntegrationServer(t, broker.NewDispatcher())
	defer conn.Close()
	payload, err := protocol.EncodeConsumeRequest(protocol.ConsumeRequest{
		Topic: "missing", PartitionID: 0, Offset: 0, MaxMessages: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	response := sendTestRequest(t, conn, protocol.Request{
		APIKey: protocol.APICONSUME, APIVersion: 1, CorrelationID: 103, Payload: payload,
	})
	if response.ErrorCode != protocol.ErrorUnknownTopic {
		t.Fatalf("ErrorCode = %d, want %d", response.ErrorCode, protocol.ErrorUnknownTopic)
	}
	_ = conn.Close()
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not exit")
	}
}

func TestMalformedRequestClosesConnection(t *testing.T) {
	conn, serverDone := startIntegrationServer(t, broker.NewDispatcher())
	defer conn.Close()
	if err := WriteFrame(conn, []byte{0x01}); err != nil {
		t.Fatalf("failed to write malformed frame: %v", err)
	}
	_, err := ReadFrame(conn)
	if err == nil {
		t.Fatal("expected connection closure after malformed request")
	}
	if !errors.Is(err, io.EOF) {
		var netErr net.Error
		if !errors.As(err, &netErr) || netErr.Timeout() {
			t.Fatalf("expected connection closure, got: %v", err)
		}
	}
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not exit after malformed request")
	}
}
