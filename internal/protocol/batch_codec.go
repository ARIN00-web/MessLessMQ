package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	batchHeaderSize = 4
	batchItemHeader = 4

	MaxBatchMessages = 1000
	MaxBatchSize     = 10 * 1024 * 1024 // 10 MiB
)

// EncodeBatch encodes an ordered batch of messages.
func EncodeBatch(batch MessageBatch) ([]byte, error) {
	count := len(batch.Messages)

	if count == 0 {
		return nil, fmt.Errorf("batch must contain at least one message")
	}

	if count > MaxBatchMessages {
		return nil, fmt.Errorf("batch exceeds maximum message count")
	}

	// Validate the total size before encoding individual messages.
	totalSize := batchHeaderSize

	for _, msg := range batch.Messages {
		if msg.Topic == "" {
			return nil, fmt.Errorf("topic cannot be empty")
		}

		if len(msg.Topic) > maxTopicSize {
			return nil, fmt.Errorf("topic is too large")
		}

		if len(msg.Key) > maxKeySize {
			return nil, fmt.Errorf("key is too large")
		}

		messageSize := messageHeaderSize +
			len(msg.Topic) +
			len(msg.Key) +
			len(msg.Value)

		if messageSize > MaxMessageSize {
			return nil, fmt.Errorf("message exceeds maximum size")
		}

		totalSize += batchItemHeader + messageSize

		if totalSize > MaxBatchSize {
			return nil, fmt.Errorf("batch exceeds maximum size")
		}
	}

	data := make([]byte, totalSize)
	binary.BigEndian.PutUint32(data[0:4], uint32(count))

	offset := batchHeaderSize

	for _, msg := range batch.Messages {
		encoded, err := EncodeMessage(msg)
		if err != nil {
			return nil, fmt.Errorf("encode batch message: %w", err)
		}

		binary.BigEndian.PutUint32(
			data[offset:offset+batchItemHeader],
			uint32(len(encoded)),
		)
		offset += batchItemHeader

		copy(data[offset:], encoded)
		offset += len(encoded)
	}

	return data, nil
}

// DecodeBatch decodes a complete batch or returns an error.
func DecodeBatch(data []byte) (MessageBatch, error) {
	if len(data) < batchHeaderSize {
		return MessageBatch{}, fmt.Errorf("batch header is incomplete")
	}

	if len(data) > MaxBatchSize {
		return MessageBatch{}, fmt.Errorf("batch exceeds maximum size")
	}

	count := binary.BigEndian.Uint32(data[0:4])

	if count == 0 {
		return MessageBatch{}, fmt.Errorf("batch must contain at least one message")
	}

	if count > MaxBatchMessages {
		return MessageBatch{}, fmt.Errorf("batch exceeds maximum message count")
	}

	messages := make([]Message, 0, int(count))
	offset := batchHeaderSize

	for i := uint32(0); i < count; i++ {
		if len(data)-offset < batchItemHeader {
			return MessageBatch{}, fmt.Errorf(
				"batch message %d has an incomplete length header", i,
			)
		}

		messageLength := binary.BigEndian.Uint32(
			data[offset : offset+batchItemHeader],
		)
		offset += batchItemHeader

		remaining := len(data) - offset

		if messageLength > uint32(remaining) {
			return MessageBatch{}, fmt.Errorf(
				"batch message %d exceeds remaining data", i,
			)
		}

		if messageLength > MaxMessageSize {
			return MessageBatch{}, fmt.Errorf(
				"batch message %d exceeds maximum message size", i,
			)
		}

		end := offset + int(messageLength)

		msg, err := DecodeMessage(data[offset:end])
		if err != nil {
			return MessageBatch{}, fmt.Errorf(
				"decode batch message %d: %w", i, err,
			)
		}

		messages = append(messages, msg)
		offset = end
	}

	if offset != len(data) {
		return MessageBatch{}, fmt.Errorf(
			"batch contains %d unexpected trailing bytes",
			len(data)-offset,
		)
	}

	return MessageBatch{Messages: messages}, nil
}
