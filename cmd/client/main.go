package main

import (
	"log"
	"net"

	"github.com/ARIN00-web/messlessmq/internal/network"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	log.Println("connected to MessLessMQ")

	/*request := protocol.Request{
		APIKey:        protocol.APICONSUME,
		APIVersion:    1,
		CorrelationID: 1,
		Payload:       []byte("test"),
	}

	frame, err := protocol.EncodeRequest(request)
	if err != nil {
		log.Fatal(err)
	}

	if err := network.WriteFrame(conn, frame); err != nil {
		log.Fatal(err)
	}

	log.Println("sent CONSUME request")

	responseFrame, err := network.ReadFrame(conn)
	if err != nil {
		log.Fatal(err)
	}

	response, err := protocol.DecodeResponse(responseFrame)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"received response: correlation=%d error=%d payload=%q\n",
		response.CorrelationID,
		response.ErrorCode,
		response.Payload,
	)
*/


requests := []protocol.Request{
	{
		APIKey:        999,
		APIVersion:    1,
		CorrelationID: 1,
		Payload:       []byte("unknown API"),
	},
	{
		APIKey:        protocol.APICONSUME,
		APIVersion:    1,
		CorrelationID: 2,
		Payload:       []byte("test"),
	},
}

for _, request := range requests {
	frame, err := protocol.EncodeRequest(request)
	if err != nil {
		log.Fatal(err)
	}

	if err := network.WriteFrame(conn, frame); err != nil {
		log.Fatal(err)
	}

	log.Printf("sent request correlation=%d", request.CorrelationID)

	responseFrame, err := network.ReadFrame(conn)
	if err != nil {
		log.Fatal(err)
	}

	response, err := protocol.DecodeResponse(responseFrame)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"received response: correlation=%d error=%d payload=%q",
		response.CorrelationID,
		response.ErrorCode,
		response.Payload,
	)
}
}