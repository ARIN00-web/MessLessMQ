package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/recordmeta"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

const ProduceAPIVersion uint16 = 1

type PartitionAppendFunc func(partition *topic.Partition, topicName string, value []byte, timestamp int64, ack protocol.AckMode) (uint64, error)

type ProduceHandler struct {
	registry *topic.Registry
	appendFn PartitionAppendFunc

	mu          sync.Mutex
	assignments map[string]*PartitionAssignment
}

func NewProduceHandler(registries ...*topic.Registry) *ProduceHandler {
	registry := topic.NewRegistry()
	if len(registries) > 0 && registries[0] != nil {
		registry = registries[0]
	}
	return &ProduceHandler{registry: registry, assignments: make(map[string]*PartitionAssignment)}
}

// SetPartitionAppendFunc installs the broker runtime's append policy. It must
// be called during startup, before the handler is used by request goroutines.
// A nil function restores the local-only append behaviour.
func (h *ProduceHandler) SetPartitionAppendFunc(fn PartitionAppendFunc) {
	if h == nil {
		return
	}
	h.appendFn = fn
}

type plannedMessage struct {
	message   protocol.Message
	partition *topic.Partition
}

func (h *ProduceHandler) assignmentFor(t *topic.Topic) (*PartitionAssignment, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if assignment := h.assignments[t.Name()]; assignment != nil {
		return assignment, nil
	}
	assignment, err := NewPartitionAssignment(t)
	if err != nil {
		return nil, err
	}
	h.assignments[t.Name()] = assignment
	return assignment, nil
}

func (h *ProduceHandler) Handle(req protocol.Request) ([]byte, error) {
	if h == nil || h.registry == nil {
		return nil, fmt.Errorf("produce handler is not initialized: %w", protocol.ErrInvalidRequest)
	}
	if req.APIKey != protocol.APIPRODUCE {
		return nil, fmt.Errorf("expected PRODUCE API key: %w", protocol.ErrInvalidRequest)
	}
	if req.APIVersion != ProduceAPIVersion {
		return nil, fmt.Errorf("produce API version %d: %w", req.APIVersion, protocol.ErrUnsupportedVersion)
	}
	produceRequest, err := protocol.DecodeProduceRequest(req.Payload)
	if err != nil {
		return nil, err
	}

	// Resolve and validate the entire batch before the first append. Storage
	// failures can still produce partial progress; this API is not transactional.
	planned := make([]plannedMessage, 0, len(produceRequest.Batch.Messages))
	for i, message := range produceRequest.Batch.Messages {
		registeredTopic, getErr := h.registry.Get(message.Topic)
		if getErr != nil {
			return nil, fmt.Errorf("message %d topic %q: %w", i, message.Topic, getErr)
		}
		assignment, assignmentErr := h.assignmentFor(registeredTopic)
		if assignmentErr != nil {
			return nil, fmt.Errorf("message %d partition assignment: %w", i, assignmentErr)
		}
		partition, assignErr := assignment.Assign(message.Key)
		if assignErr != nil {
			return nil, fmt.Errorf("message %d partition routing: %w", i, assignErr)
		}
		if message.Timestamp <= 0 {
			message.Timestamp = time.Now().UnixMilli()
		}
		planned = append(planned, plannedMessage{message: message, partition: partition})
	}

	response := protocol.ProduceResponse{Records: make([]protocol.ProduceRecordResult, 0, len(planned))}
	for i, item := range planned {
		storedValue, encodeErr := recordmeta.EncodeMessage(item.message.Key, item.message.Value)
		if encodeErr != nil {
			return nil, fmt.Errorf("encode message %d: %w", i, encodeErr)
		}
		var offset uint64
		if h.appendFn != nil {
			offset, err = h.appendFn(item.partition, item.message.Topic, storedValue, item.message.Timestamp, produceRequest.AckMode)
		} else {
			switch produceRequest.AckMode {
			case protocol.AckNone:
				offset, err = item.partition.AppendAt(storedValue, item.message.Timestamp)
			case protocol.AckLeader, protocol.AckAll:
				// Without a distributed appender, this broker is the entire ISR.
				offset, err = item.partition.AppendAtSync(storedValue, item.message.Timestamp)
			default:
				err = protocol.ErrInvalidAckMode
			}
		}
		if err != nil {
			return nil, fmt.Errorf("append message %d after %d successful appends: %w", i, len(response.Records), err)
		}
		response.Records = append(response.Records, protocol.ProduceRecordResult{
			Topic: item.message.Topic, PartitionID: item.partition.ID(), Offset: offset,
		})
	}
	return protocol.EncodeProduceResponse(response)
}
