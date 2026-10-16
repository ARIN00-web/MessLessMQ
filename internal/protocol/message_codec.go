package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	messageHeaderSize = 18
	MaxMessageSize    = 10 * 1024 * 1024 // 10 MiB

	maxTopicSize = 1<<16 - 1
	maxKeySize   = 1<<31 - 1
)

func EncodeMessage(msg Message) ([]byte, error) {
	if msg.Topic == "" {
		return nil, fmt.Errorf("topic cannot be empty")
	}

	if len(msg.Topic) > maxTopicSize {
		return nil, fmt.Errorf("topic is too large")
	}

	if len(msg.Key) > maxKeySize {
		return nil, fmt.Errorf("key is too large")
	}

	if len(msg.Value) > MaxMessageSize {
		return nil, fmt.Errorf("value exceeds maximum message size")
	}

	keyLength := len(msg.Key)
	if msg.Key == nil {
		keyLength = -1
	}

	totalSize := messageHeaderSize +
		len(msg.Topic) +
		len(msg.Key) +
		len(msg.Value)

	if totalSize > MaxMessageSize {
		return nil, fmt.Errorf("message exceeds maximum size")
	}

	data := make([]byte, totalSize)

	binary.BigEndian.PutUint16(data[0:2], uint16(len(msg.Topic)))

	binary.BigEndian.PutUint32(data[2:6], uint32(int32(keyLength)))

	binary.BigEndian.PutUint32(data[6:10], uint32(len(msg.Value)))

	binary.BigEndian.PutUint64(data[10:18], uint64(msg.Timestamp))

	offset := messageHeaderSize

	copy(data[offset:], msg.Topic)
	offset += len(msg.Topic)

	if msg.Key != nil {
		copy(data[offset:], msg.Key)
		offset += len(msg.Key)
	}

	copy(data[offset:], msg.Value)

	return data, nil
}

func DecodeMessage(data []byte) (Message, error) {
	if len(data) < messageHeaderSize {
		return Message{}, fmt.Errorf(
			"message too short: got %d bytes, need at least %d",
			len(data),
			messageHeaderSize,
		)
	}

	if len(data) > MaxMessageSize {
		return Message{}, fmt.Errorf("message exceeds maximum size")
	}

	topicLength := int(binary.BigEndian.Uint16(data[0:2]))
	keyLength := int(int32(binary.BigEndian.Uint32(data[2:6])))
	valueLength := int(binary.BigEndian.Uint32(data[6:10]))
	timestamp := int64(binary.BigEndian.Uint64(data[10:18]))

	if topicLength == 0 {
		return Message{}, fmt.Errorf("topic cannot be empty")
	}

	if keyLength < -1 {
		return Message{}, fmt.Errorf("invalid key length: %d", keyLength)
	}

	if valueLength < 0 {
		return Message{}, fmt.Errorf("invalid value length")
	}

	expectedSize := messageHeaderSize + topicLength + valueLength
	if keyLength > 0 {
		expectedSize += keyLength
	}

	if expectedSize != len(data) {
		return Message{}, fmt.Errorf(
			"invalid message size: got %d bytes, expected %d",
			len(data),
			expectedSize,
		)
	}

	offset := messageHeaderSize

	topic := string(data[offset : offset+topicLength])
	offset += topicLength

	var key []byte
	if keyLength >= 0 {
		key = append([]byte{}, data[offset:offset+keyLength]...)
		offset += keyLength
	}

	value := append([]byte{}, data[offset:offset+valueLength]...)

	return Message{
		Topic:     topic,
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}, nil
}
