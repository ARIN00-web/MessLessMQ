package broker

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

var (
	ErrUnknownAPI         = errors.New("unknown API key")
	ErrHandlerUnavailable = errors.New("API handler unavailable")
)

type Dispatcher struct {
	metadataMu      sync.RWMutex
	produceHandler  Handler
	consumeHandler  Handler
	metadataHandler Handler
}

// NewDispatcher accepts the broker's shared topic registry. The variadic form
// preserves the original no-argument constructor for callers and tests that
// do not configure topics.
func NewDispatcher(registries ...*topic.Registry) *Dispatcher {
	registry := topic.NewRegistry()
	if len(registries) > 0 && registries[0] != nil {
		registry = registries[0]
	}
	return &Dispatcher{
		produceHandler: NewProduceHandler(registry),
		consumeHandler: NewConsumeHandler(registry),
	}
}

// SetPartitionAppendFunc installs the runtime append policy on the produce
// handler. Call during broker construction before serving requests.
func (d *Dispatcher) SetPartitionAppendFunc(fn PartitionAppendFunc) {
	if d == nil {
		return
	}
	if handler, ok := d.produceHandler.(*ProduceHandler); ok {
		handler.SetPartitionAppendFunc(fn)
	}
}

// SetHighWatermarkLookup installs the committed-read boundary on the consume
// handler. Call during broker construction before serving requests.
func (d *Dispatcher) SetHighWatermarkLookup(lookup HighWatermarkLookup) {
	if d == nil {
		return
	}
	if handler, ok := d.consumeHandler.(*ConsumeHandler); ok {
		handler.SetHighWatermarkLookup(lookup)
	}
}

func (d *Dispatcher) SetMetadataHandler(handler Handler) {
	if d == nil {
		return
	}
	d.metadataMu.Lock()
	d.metadataHandler = handler
	d.metadataMu.Unlock()
}

func (d *Dispatcher) Dispatch(req protocol.Request) ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: nil dispatcher", protocol.ErrInvalidRequest)
	}
	switch req.APIKey {
	case protocol.APIPRODUCE:
		return d.produceHandler.Handle(req)
	case protocol.APICONSUME:
		return d.consumeHandler.Handle(req)
	case protocol.APIMETADATA:
		d.metadataMu.RLock()
		handler := d.metadataHandler
		d.metadataMu.RUnlock()
		if handler == nil {
			return nil, ErrHandlerUnavailable
		}
		return handler.Handle(req)
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownAPI, req.APIKey)
	}
}
