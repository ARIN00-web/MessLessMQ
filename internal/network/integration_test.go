package network

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func startIntegrationServer(
	t *testing.T,
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

		HandleConnection(
			serverConn,
			broker.NewDispatcher(),
			make(chan struct{}),
		)
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatalf("failed to connect to test server: %v", err)
	}

	if err := clientConn.SetDeadline(
		time.Now().Add(2 * time.Second),
	); err != nil {
		clientConn.Close()
		t.Fatalf("failed to set client deadline: %v", err)
	}

	return clientConn, serverDone
}

func sendTestRequest(
	t *testing.T,
	conn net.Conn,
	req protocol.Request,
) protocol.Response {
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
	conn, serverDone := startIntegrationServer(t)
	defer conn.Close()

	// First request: an unknown API should return an error.
	first := sendTestRequest(t, conn, protocol.Request{
		APIKey:        999,
		APIVersion:    1,
		CorrelationID: 101,
		Payload:       []byte("unknown API"),
	})

	if first.CorrelationID != 101 {
		t.Fatalf("first correlation ID: got %d, want 101",
			first.CorrelationID)
	}

	if first.ErrorCode != protocol.ErrorUnknownAPI {
		t.Fatalf("first error code: got %d, want %d",
			first.ErrorCode, protocol.ErrorUnknownAPI)
	}

	// Second request: the same connection must remain usable.
	second := sendTestRequest(t, conn, protocol.Request{
		APIKey:        protocol.APICONSUME,
		APIVersion:    1,
		CorrelationID: 102,
		Payload:       []byte("test"),
	})

	if second.CorrelationID != 102 {
		t.Fatalf("second correlation ID: got %d, want 102",
			second.CorrelationID)
	}

	if second.ErrorCode != protocol.ErrorNone {
		t.Fatalf("second error code: got %d, want %d",
			second.ErrorCode, protocol.ErrorNone)
	}

	if string(second.Payload) != "CONSUME handler not implemented" {
		t.Fatalf("unexpected second payload: %q", second.Payload)
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

func TestMalformedRequestClosesConnection(t *testing.T) {
	conn, serverDone := startIntegrationServer(t)
	defer conn.Close()

	// A one-byte protocol message is too short to contain a request header.
	if err := WriteFrame(conn, []byte{0x01}); err != nil {
		t.Fatalf("failed to write malformed frame: %v", err)
	}

	// A malformed request cannot be decoded, so no response is expected.
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
