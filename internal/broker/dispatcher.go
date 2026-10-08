package broker

import (
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

type Dispatcher struct {
	produceHandler Handler
	consumeHandler Handler
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		produceHandler: NewProduceHandler(),
		consumeHandler: NewConsumeHandler(),
	}
}

func (d *Dispatcher) Dispatch(req protocol.Request) ([]byte, error) {
	switch req.APIKey {
	case protocol.APIPRODUCE:
		return d.produceHandler.Handle(req)

	case protocol.APICONSUME:
		return d.consumeHandler.Handle(req)

	default:
		return nil, fmt.Errorf("unknown API key: %d", req.APIKey)
	}
}