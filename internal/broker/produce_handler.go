package broker

import "github.com/ARIN00-web/messlessmq/internal/protocol"

type ProduceHandler struct{}

func NewProduceHandler() *ProduceHandler {
	return &ProduceHandler{}
}

func (h *ProduceHandler) Handle(req protocol.Request) ([]byte, error) {
	return []byte("PRODUCE handler not implemented"), nil
}