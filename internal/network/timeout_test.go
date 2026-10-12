package network

import (
	"net"
	"testing"
	"time"
)

func TestReadTimeout(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer serverConn.Close()
	defer clientConn.Close()

	if err := serverConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}

	buffer := make([]byte, 1)

	_, err := serverConn.Read(buffer)
	if err == nil {
		t.Fatal("expected read timeout, got nil")
	}

	if !isTimeoutError(err) {
		t.Fatalf("expected timeout error, got: %v", err)
	}
}

func TestWriteTimeout(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer serverConn.Close()
	defer clientConn.Close()

	if err := serverConn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("failed to set write deadline: %v", err)
	}

	largePayload := make([]byte, 1024*1024)

	_, err := serverConn.Write(largePayload)
	if err == nil {
		t.Fatal("expected write timeout, got nil")
	}

	if !isTimeoutError(err) {
		t.Fatalf("expected timeout error, got: %v", err)
	}
}
