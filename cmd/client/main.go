
package main

import (
	"log"
	"net"

	"github.com/ARIN00-web/messlessmq/internal/network"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	log.Println("connected to MessLessMQ")

	err = network.WriteFrame(conn, []byte("PING"))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("sent: PING")
}