package broker

import "github.com/ARIN00-web/messlessmq/internal/protocol"

type Handler interface {
	Handle(req protocol.Request) ([]byte, error)
}