package broker


import (
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

type Dispatcher struct{}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{}
}

func (d *Dispatcher) Dispatch(req protocol.Request) ([]byte, error) {
	switch req.APIKey {
	case protocol.APIPRODUCE:
		return d.handleProduce(req)

	case protocol.APICONSUME:
		return d.handleConsume(req)

	default:
		return nil, fmt.Errorf("unknown API key: %d", req.APIKey)
	}
}

func (d *Dispatcher) handleProduce(req protocol.Request) ([]byte, error) {
	return []byte("PRODUCE handler not implemented"), nil
}

func (d *Dispatcher) handleConsume(req protocol.Request) ([]byte, error) {
	return []byte("CONSUME handler not implemented"), nil
}