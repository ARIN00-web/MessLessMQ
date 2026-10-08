package network

import (
	"io"
	"log"
	"net"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func HandleConnection(conn net.Conn, dispatcher *broker.Dispatcher) {
	defer conn.Close()

	log.Println("client connected:", conn.RemoteAddr())

	for {
		if err := handleRequest(conn, dispatcher); err != nil {
			if err == io.EOF {
				log.Println("client disconnected:", conn.RemoteAddr())
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

	responsePayload, err := dispatcher.Dispatch(request)
	if err != nil {
		return err
	}

	response := protocol.Request{
		APIKey:        request.APIKey,
		APIVersion:    request.APIVersion,
		CorrelationID: request.CorrelationID,
		Payload:       responsePayload,
	}

	responseFrame, err := protocol.EncodeRequest(response)
	if err != nil {
		return err
	}

	return WriteFrame(conn, responseFrame)
}