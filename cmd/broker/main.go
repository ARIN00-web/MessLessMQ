
package main

import (
	"log"
	"net"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/network"
)

func main() {
	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	dispatcher := broker.NewDispatcher()

	log.Println("MessLessMQ broker listening on :9092")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("failed to accept connection:", err)
			continue
		}

		go network.HandleConnection(conn, dispatcher)
	}
}
