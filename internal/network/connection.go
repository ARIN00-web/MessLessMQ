package network

import (
	"errors"
	"io"
	"log"
	"net"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func HandleConnection(
	conn net.Conn,
	dispatcher *broker.Dispatcher,
	shutdown <-chan struct{},
) {
	defer conn.Close()

	log.Println("client connected:", conn.RemoteAddr())

	for {
		// Do not begin another request after shutdown starts.
		select {
		case <-shutdown:
			log.Println("stopping connection:", conn.RemoteAddr())
			return
		default:
		}

		if err := conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			log.Println("failed to set read deadline:", err)
			return
		}

		if err := handleRequest(conn, dispatcher); err != nil {
			if errors.Is(err, io.EOF) {
				log.Println("client disconnected:", conn.RemoteAddr())
			} else if isTimeoutError(err) {
				log.Println("client timed out:", conn.RemoteAddr())
			} else {
				log.Println("connection error:", err)
			}

			return
		}
	}
}

func handleRequest(conn net.Conn, dispatcher *broker.Dispatcher) error {
	frame, err := ReadFrame(conn)
	if err != nil {
		return err
	}

	request, err := protocol.DecodeRequest(frame)
	if err != nil {
		return err
	}

	responsePayload, dispatchErr := dispatcher.Dispatch(request)

	response := protocol.Response{
		APIKey:        request.APIKey,
		APIVersion:    request.APIVersion,
		CorrelationID: request.CorrelationID,
		ErrorCode:     errorCodeFromError(dispatchErr),
		Payload:       responsePayload,
	}

	if dispatchErr != nil {
		response.Payload = []byte("request failed")
	}

	responseFrame, err := protocol.EncodeResponse(response)
	if err != nil {
		return err
	}

	if err := conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
		return err
	}

	return WriteFrame(conn, responseFrame)
}

func errorCodeFromError(err error) uint16 {
	if err == nil {
		return protocol.ErrorNone
	}

	if errors.Is(err, broker.ErrUnknownAPI) {
		return protocol.ErrorUnknownAPI
	}

	return protocol.ErrorInternal
}

func isTimeoutError(err error) bool {
	var netErr net.Error

	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	return false
}
