package interbroker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const DefaultTimeout = 5 * time.Second

type RemoteError struct{ Message string }

func (e *RemoteError) Error() string { return "interbroker remote error: " + e.Message }

type Transport struct {
	Dialer  net.Dialer
	Timeout time.Duration
}

func NewTransport(timeout time.Duration) *Transport {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Transport{Dialer: net.Dialer{Timeout: timeout}, Timeout: timeout}
}

// RoundTrip uses one TCP connection per request. This intentionally simple
// first transport makes framing, deadlines, and request correlation explicit.
func (t *Transport) RoundTrip(ctx context.Context, address string, request Envelope) (Envelope, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if t == nil || address == "" || request.IsResponse {
		return Envelope{}, ErrInvalidEnvelope
	}
	conn, err := t.Dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return Envelope{}, fmt.Errorf("dial peer %q: %w", address, err)
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	defer conn.Close()
	timeout := t.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return Envelope{}, err
	}
	if err := writeFrame(conn, request); err != nil {
		return Envelope{}, fmt.Errorf("write peer request: %w", err)
	}
	response, err := readFrame(conn)
	if err != nil {
		return Envelope{}, fmt.Errorf("read peer response: %w", err)
	}
	if !response.IsResponse || response.RequestID != request.RequestID || response.Type != request.Type ||
		response.SourceID != request.TargetID || response.TargetID != request.SourceID {
		return Envelope{}, ErrUnexpectedReply
	}
	if response.Error != "" {
		return Envelope{}, &RemoteError{Message: response.Error}
	}
	return response, nil
}

func writeFrame(writer io.Writer, envelope Envelope) error {
	payload, err := EncodeEnvelope(envelope)
	if err != nil {
		return err
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeAll(writer, header[:]); err != nil {
		return err
	}
	return writeAll(writer, payload)
}

func readFrame(reader io.Reader) (Envelope, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return Envelope{}, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length < peerHeaderSize || length > MaxPeerFrame {
		return Envelope{}, ErrFrameTooLarge
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return Envelope{}, err
	}
	return DecodeEnvelope(payload)
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func isClosedNetworkError(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
