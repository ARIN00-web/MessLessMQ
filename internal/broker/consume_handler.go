package broker

import "github.com/ARIN00-web/messlessmq/internal/protocol"

type ConsumeHandler struct{}

func NewConsumeHandler() *ConsumeHandler {
	return &ConsumeHandler{}
}

func (h *ConsumeHandler) Handle(req protocol.Request) ([]byte, error) {
	return []byte("CONSUME handler not implemented"), nil
}