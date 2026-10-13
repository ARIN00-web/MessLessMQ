package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/network"
)

const shutdownTimeout = 10 * time.Second

func main() {
	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Fatal(err)
	}

	dispatcher := broker.NewDispatcher()

	shutdown := make(chan struct{})

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		conns = make(map[net.Conn]struct{})
	)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	go func() {
		sig := <-signals
		log.Println("shutdown signal received:", sig)

		close(shutdown)

		// Unblock the Accept call.
		if err := listener.Close(); err != nil {
			log.Println("failed to close listener:", err)
		}
	}()

	log.Println("MessLessMQ broker listening on :9092")

acceptLoop:
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-shutdown:
				break acceptLoop
			default:
				log.Println("failed to accept connection:", err)
				continue
			}
		}

		// Shutdown may have started while Accept was returning.
		select {
		case <-shutdown:
			conn.Close()
			break acceptLoop
		default:
		}

		mu.Lock()
		conns[conn] = struct{}{}
		wg.Add(1)
		mu.Unlock()

		go func(conn net.Conn) {
			defer wg.Done()

			defer func() {
				mu.Lock()
				delete(conns, conn)
				mu.Unlock()
			}()

			network.HandleConnection(conn, dispatcher, shutdown)
		}(conn)
	}

	log.Println("stopped accepting connections; draining active connections")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(shutdownTimeout)
	defer timer.Stop()

	select {
	case <-done:
		log.Println("all connections closed cleanly")

	case <-timer.C:
		log.Println("shutdown deadline reached; closing remaining connections")

		mu.Lock()
		for conn := range conns {
			if err := conn.Close(); err != nil {
				log.Println("failed to close connection:", err)
			}
		}
		mu.Unlock()

		// Closing sockets unblocks network reads and writes.
		wg.Wait()
	}

	log.Println("MessLessMQ broker stopped")
}
