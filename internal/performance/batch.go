package performance

import (
	"errors"
)

var (
	ErrInvalidBatchLimits   = errors.New("batch limits must be positive")
	ErrBatchMessageTooLarge = errors.New("message exceeds batch byte limit")
)

// Batch is an immutable-by-convention group of payloads emitted by BatchBuilder.
// The builder copies messages on Add, so callers may reuse their input buffers.
type Batch struct {
	Messages [][]byte
	Bytes    int
}

// BatchBuilder groups messages while respecting both a record-count and byte
// ceiling. It is intentionally not concurrency-safe: give each producer or
// partition its own builder, or protect it with a mutex.
type BatchBuilder struct {
	maxMessages int
	maxBytes    int
	messages    [][]byte
	bytes       int
}

func NewBatchBuilder(maxMessages, maxBytes int) (*BatchBuilder, error) {
	if maxMessages <= 0 || maxBytes <= 0 {
		return nil, ErrInvalidBatchLimits
	}
	return &BatchBuilder{
		maxMessages: maxMessages,
		maxBytes:    maxBytes,
		messages:    make([][]byte, 0, maxMessages),
	}, nil
}

// Add copies message into the current batch. If the message would exceed a
// limit, Add first emits the previous batch and starts a new one with message.
// The returned batch is nil when no prior batch needed to be emitted.
func (b *BatchBuilder) Add(message []byte) (*Batch, error) {
	if b == nil {
		return nil, ErrInvalidBatchLimits
	}
	if len(message) > b.maxBytes {
		return nil, ErrBatchMessageTooLarge
	}

	var emitted *Batch
	if len(b.messages) > 0 && (len(b.messages) >= b.maxMessages || b.bytes > b.maxBytes-len(message)) {
		emitted = b.Flush()
	}

	owned := append([]byte(nil), message...)
	b.messages = append(b.messages, owned)
	b.bytes += len(owned)
	return emitted, nil
}

// Flush returns the accumulated batch and resets the builder. It returns nil
// when the builder is empty. The returned payload slices remain valid after
// the builder is reused.
func (b *BatchBuilder) Flush() *Batch {
	if b == nil || len(b.messages) == 0 {
		return nil
	}
	out := &Batch{Messages: b.messages, Bytes: b.bytes}
	b.messages = make([][]byte, 0, b.maxMessages)
	b.bytes = 0
	return out
}

func (b *BatchBuilder) Len() int {
	if b == nil {
		return 0
	}
	return len(b.messages)
}

func (b *BatchBuilder) Bytes() int {
	if b == nil {
		return 0
	}
	return b.bytes
}
