package broker

import (
	"fmt"
	"sync"

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
type HighWatermarkLookup func(topicName string, partitionID uint32) (uint64, error)

type ConsumeHandler struct {
	registry      *topic.Registry
	mu            sync.RWMutex
	highWatermark HighWatermarkLookup
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

// SetHighWatermarkLookup restricts fetches to records below the persisted
// exclusive commit boundary. Install it during broker construction before
// serving requests. A nil lookup retains the standalone handler's historical
// behaviour of exposing the local log end.
func (h *ConsumeHandler) SetHighWatermarkLookup(lookup HighWatermarkLookup) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.highWatermark = lookup
	h.mu.Unlock()
}

func (h *ConsumeHandler) committedEnd(topicName string, partitionID uint32, logEnd uint64) (uint64, error) {
	h.mu.RLock()
	lookup := h.highWatermark
	h.mu.RUnlock()
	if lookup == nil {
		return logEnd, nil
	}
	return lookup(topicName, partitionID)
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

	startOffset, physicalLogEnd, err := partition.OffsetRange()
	if err != nil {
		return nil, fmt.Errorf("read partition range: %w", err)
	}
	committedEnd, err := h.committedEnd(consumeReq.Topic, consumeReq.PartitionID, physicalLogEnd)
	if err != nil {
		return nil, fmt.Errorf("read committed boundary: %w", err)
	}
	if committedEnd < startOffset || committedEnd > physicalLogEnd {
		return nil, fmt.Errorf("committed boundary %d outside local range [%d,%d)", committedEnd, startOffset, physicalLogEnd)
	}
	if consumeReq.Offset > committedEnd {
		return nil, fmt.Errorf("requested offset %d exceeds committed end %d: %w", consumeReq.Offset, committedEnd, consumer.ErrOffsetOutOfRange)
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
	// The physical log may contain an uncommitted tail. Never expose those
	// records or report the physical end offset to clients.
	visible := result.Records[:0]
	for _, record := range result.Records {
		if record.Offset < committedEnd {
			visible = append(visible, record)
		}
	}
	result.Records = visible
	if result.NextOffset > committedEnd {
		result.NextOffset = committedEnd
	}
	result.LogEndOffset = committedEnd

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
