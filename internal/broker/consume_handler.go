package broker

import (
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/consumer"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

const (
	ConsumeAPIVersion  uint16 = 1
	ConsumeAPIVersion2 uint16 = 2
)

// ConsumeHandler serves stateless fetch requests against a registered topic.
// The client supplies the next offset on every request; durable consumer-group
// offsets are intentionally outside Phase 9.
type ConsumeHandler struct {
	registry *topic.Registry
}

// NewConsumeHandler creates a consume handler. When no registry is supplied,
// it creates an empty in-memory registry. A broker should normally pass its
// shared registry explicitly.
func NewConsumeHandler(registries ...*topic.Registry) *ConsumeHandler {
	registry := topic.NewRegistry()
	if len(registries) > 0 && registries[0] != nil {
		registry = registries[0]
	}
	return &ConsumeHandler{registry: registry}
}

func (h *ConsumeHandler) Handle(req protocol.Request) ([]byte, error) {
	if h == nil || h.registry == nil {
		return nil, fmt.Errorf("%w: consume handler is not initialized", protocol.ErrInvalidRequest)
	}
	if req.APIKey != protocol.APICONSUME {
		return nil, fmt.Errorf("%w: expected CONSUME API key", protocol.ErrInvalidRequest)
	}
	if req.APIVersion != ConsumeAPIVersion && req.APIVersion != ConsumeAPIVersion2 {
		return nil, fmt.Errorf("%w: consume API version %d", protocol.ErrUnsupportedVersion, req.APIVersion)
	}

	consumeReq, err := protocol.DecodeConsumeRequest(req.Payload)
	if err != nil {
		return nil, err
	}

	registeredTopic, err := h.registry.Get(consumeReq.Topic)
	if err != nil {
		return nil, err
	}

	partition, err := registeredTopic.Partition(consumeReq.PartitionID)
	if err != nil {
		return nil, err
	}

	reader, err := consumer.New(partition)
	if err != nil {
		return nil, fmt.Errorf("create request reader: %w", err)
	}

	if err := reader.SetPosition(
		consumeReq.Topic,
		consumeReq.PartitionID,
		consumeReq.Offset,
	); err != nil {
		return nil, err
	}

	result, err := reader.Fetch(
		consumeReq.Topic,
		consumeReq.PartitionID,
		consumeReq.MaxMessages,
	)
	if err != nil {
		return nil, err
	}

	response := protocol.ConsumeResponse{
		StartOffset:  result.StartOffset,
		LogEndOffset: result.LogEndOffset,
		NextOffset:   result.NextOffset,
		Records:      make([]protocol.ConsumeRecord, 0, len(result.Records)),
	}
	for _, record := range result.Records {
		response.Records = append(response.Records, protocol.ConsumeRecord{
			Offset: record.Offset, Timestamp: record.Timestamp,
			Key: record.Key, Value: record.Value,
		})
	}
	if req.APIVersion == ConsumeAPIVersion2 {
		return protocol.EncodeConsumeResponseV2(response)
	}
	return protocol.EncodeConsumeResponse(response)
}
