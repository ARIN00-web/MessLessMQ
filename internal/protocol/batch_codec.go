package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	batchHeaderSize  = 4
	batchMessageSize = 4

	MaxBatchMessages = 1000
	MaxBatchSize     = 10 * 1024 * 1024 // 10 MiB
)

// EncodeBatch serializes a batch of messages.
//
// Wire format:
//
//	[MessageCount: 4 bytes]
//	Repeated for each message:
//	    [MessageLength: 4 bytes]
//	    [EncodedMessage: MessageLength bytes]
//
// All integer fields use big-endian byte order.
// Encoding fails if any message is invalid or the batch exceeds its limits.
func EncodeBatch(batch MessageBatch) ([]byte, error) {
	messageCount := len(batch.Messages)

	if messageCount == 0 {
		return nil, fmt.Errorf(
			"%w: batch cannot be empty",
			ErrInvalidRequest,
		)
	}

	if messageCount > MaxBatchMessages {
		return nil, fmt.Errorf(
			"%w: batch contains %d messages; maximum is %d",
			ErrMessageTooLarge,
			messageCount,
			MaxBatchMessages,
		)
	}

	encodedMessages := make([][]byte, 0, messageCount)
	totalSize := batchHeaderSize

	for i, msg := range batch.Messages {
		encoded, err := EncodeMessage(msg)
		if err != nil {
			return nil, fmt.Errorf("encode message %d: %w", i, err)
		}

		// Check the size before adding it to prevent an oversized batch.
		if len(encoded) > MaxBatchSize ||
			totalSize > MaxBatchSize-batchMessageSize-len(encoded) {
			return nil, fmt.Errorf(
				"%w: batch exceeds maximum size of %d bytes",
				ErrMessageTooLarge,
				MaxBatchSize,
			)
		}

		encodedMessages = append(encodedMessages, encoded)
		totalSize += batchMessageSize + len(encoded)
	}

	data := make([]byte, totalSize)

	binary.BigEndian.PutUint32(data[0:4], uint32(messageCount))

	offset := batchHeaderSize

	for _, encoded := range encodedMessages {
		binary.BigEndian.PutUint32(
			data[offset:offset+batchMessageSize],
			uint32(len(encoded)),
		)
		offset += batchMessageSize

		copy(data[offset:], encoded)
		offset += len(encoded)
	}

	return data, nil
}

// DecodeBatch deserializes a batch and validates its structure.
//
// It rejects malformed batches instead of returning a partially decoded batch.
func DecodeBatch(data []byte) (MessageBatch, error) {
	if len(data) < batchHeaderSize {
		return MessageBatch{}, fmt.Errorf(
			"%w: batch header is incomplete",
			ErrInvalidRequest,
		)
	}

	if len(data) > MaxBatchSize {
		return MessageBatch{}, fmt.Errorf(
			"%w: batch exceeds maximum size of %d bytes",
			ErrMessageTooLarge,
			MaxBatchSize,
		)
	}

	messageCount := binary.BigEndian.Uint32(data[0:4])

	if messageCount == 0 {
		return MessageBatch{}, fmt.Errorf(
			"%w: batch cannot be empty",
			ErrInvalidRequest,
		)
	}

	if messageCount > MaxBatchMessages {
		return MessageBatch{}, fmt.Errorf(
			"%w: batch contains %d messages; maximum is %d",
			ErrMessageTooLarge,
			messageCount,
			MaxBatchMessages,
		)
	}

	// Every message requires at least its 4-byte length prefix.
	// Reject impossible counts before allocating the message slice.
	remaining := len(data) - batchHeaderSize
	if uint64(messageCount)*uint64(batchMessageSize) > uint64(remaining) {
		return MessageBatch{}, fmt.Errorf(
			"%w: batch declares %d messages but lacks their length prefixes",
			ErrInvalidRequest,
			messageCount,
		)
	}

	messages := make([]Message, 0, int(messageCount))
	offset := batchHeaderSize

	for i := uint32(0); i < messageCount; i++ {
		if len(data)-offset < batchMessageSize {
			return MessageBatch{}, fmt.Errorf(
				"%w: message %d length prefix is incomplete",
				ErrInvalidRequest,
				i,
			)
		}

		messageLength := binary.BigEndian.Uint32(
			data[offset : offset+batchMessageSize],
		)
		offset += batchMessageSize

		remaining = len(data) - offset

		if uint64(messageLength) > uint64(remaining) {
			return MessageBatch{}, fmt.Errorf(
				"%w: message %d declares %d bytes but only %d remain",
				ErrInvalidRequest,
				i,
				messageLength,
				remaining,
			)
		}

		if messageLength > MaxBatchSize {
			return MessageBatch{}, fmt.Errorf(
				"%w: message %d exceeds maximum batch size",
				ErrMessageTooLarge,
				i,
			)
		}

		messageEnd := offset + int(messageLength)
		messageData := data[offset:messageEnd]

		msg, err := DecodeMessage(messageData)
		if err != nil {
			return MessageBatch{}, fmt.Errorf(
				"decode message %d: %w",
				i,
				err,
			)
		}

		messages = append(messages, msg)
		offset = messageEnd
	}

	if offset != len(data) {
		return MessageBatch{}, fmt.Errorf(
			"%w: batch contains %d unexpected trailing bytes",
			ErrInvalidRequest,
			len(data)-offset,
		)
	}

	return MessageBatch{Messages: messages}, nil
}
