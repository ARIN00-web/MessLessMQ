package protocol

import (
	"encoding/binary"
	"fmt"
)

// ProduceRecordResult identifies the partition and offset assigned to one
// message in a produce request. Results preserve request order.
type ProduceRecordResult struct {
	Topic       string
	PartitionID uint32
	Offset      uint64
}

type ProduceResponse struct {
	Records []ProduceRecordResult
}

// EncodeProduceResponse encodes the per-record append results.
// Wire format: [count:4], then [topic length:2][topic][partition:4][offset:8].
func EncodeProduceResponse(response ProduceResponse) ([]byte, error) {
	if len(response.Records) == 0 || len(response.Records) > MaxBatchMessages {
		return nil, fmt.Errorf("%w: invalid produce result count", ErrInvalidRequest)
	}
	total := 4
	for _, record := range response.Records {
		if record.Topic == "" || len(record.Topic) > 249 {
			return nil, fmt.Errorf("%w: invalid produce result topic", ErrInvalidRequest)
		}
		total += 2 + len(record.Topic) + 4 + 8
	}
	if total > MaxBatchSize {
		return nil, ErrMessageTooLarge
	}
	data := make([]byte, total)
	binary.BigEndian.PutUint32(data[:4], uint32(len(response.Records)))
	cursor := 4
	for _, record := range response.Records {
		binary.BigEndian.PutUint16(data[cursor:cursor+2], uint16(len(record.Topic)))
		cursor += 2
		copy(data[cursor:], record.Topic)
		cursor += len(record.Topic)
		binary.BigEndian.PutUint32(data[cursor:cursor+4], record.PartitionID)
		cursor += 4
		binary.BigEndian.PutUint64(data[cursor:cursor+8], record.Offset)
		cursor += 8
	}
	return data, nil
}

func DecodeProduceResponse(data []byte) (ProduceResponse, error) {
	if len(data) < 4 || len(data) > MaxBatchSize {
		return ProduceResponse{}, fmt.Errorf("%w: invalid produce response length", ErrInvalidRequest)
	}
	count := binary.BigEndian.Uint32(data[:4])
	if count == 0 || count > MaxBatchMessages || uint64(count)*14 > uint64(len(data)-4) {
		return ProduceResponse{}, fmt.Errorf("%w: invalid produce response count", ErrInvalidRequest)
	}
	response := ProduceResponse{Records: make([]ProduceRecordResult, 0, count)}
	cursor := 4
	for i := uint32(0); i < count; i++ {
		if len(data)-cursor < 2 {
			return ProduceResponse{}, ErrInvalidRequest
		}
		length := int(binary.BigEndian.Uint16(data[cursor : cursor+2]))
		cursor += 2
		if length == 0 || length > 249 || len(data)-cursor < length+12 {
			return ProduceResponse{}, ErrInvalidRequest
		}
		topic := string(data[cursor : cursor+length])
		cursor += length
		if err := validateConsumeTopicName(topic); err != nil {
			return ProduceResponse{}, err
		}
		partition := binary.BigEndian.Uint32(data[cursor : cursor+4])
		cursor += 4
		offset := binary.BigEndian.Uint64(data[cursor : cursor+8])
		cursor += 8
		response.Records = append(response.Records, ProduceRecordResult{Topic: topic, PartitionID: partition, Offset: offset})
	}
	if cursor != len(data) {
		return ProduceResponse{}, fmt.Errorf("%w: trailing produce response bytes", ErrInvalidRequest)
	}
	return response, nil
}
