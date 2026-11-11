package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	// MaxConsumeMessages limits the number of records in one fetch request.
	MaxConsumeMessages uint32 = 500

	consumeRequestFixedSize   = 2 + 4 + 8 + 4
	consumeResponseHeaderSize = 8 + 8 + 8 + 4
	consumeRecordHeaderSize   = 8 + 8 + 4
)

// ConsumeRequest asks the broker to fetch records from a topic partition.
// Offset is the next partition-local offset the caller wants to read.
type ConsumeRequest struct {
	Topic       string
	PartitionID uint32
	Offset      uint64
	MaxMessages uint32
}

// ConsumeRecord is a record returned by a consume response.
type ConsumeRecord struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// ConsumeResponse describes a bounded read from one partition.
// StartOffset is the earliest retained offset; LogEndOffset is the next
// offset after the latest stored record; NextOffset follows the returned
// records and can be used for the next fetch.
type ConsumeResponse struct {
	StartOffset  uint64
	LogEndOffset uint64
	NextOffset   uint64
	Records      []ConsumeRecord
}

// EncodeConsumeRequest encodes a consume request.
//
// Wire format:
// [TopicLength: 2][Topic][PartitionID: 4][Offset: 8][MaxMessages: 4]
func EncodeConsumeRequest(req ConsumeRequest) ([]byte, error) {
	if err := validateConsumeTopicName(req.Topic); err != nil {
		return nil, err
	}
	if req.MaxMessages == 0 || req.MaxMessages > MaxConsumeMessages {
		return nil, fmt.Errorf("%w: must be between 1 and %d", ErrInvalidFetchLimit, MaxConsumeMessages)
	}

	data := make([]byte, 2+len(req.Topic)+4+8+4)
	binary.BigEndian.PutUint16(data[:2], uint16(len(req.Topic)))
	copy(data[2:], req.Topic)
	offset := 2 + len(req.Topic)
	binary.BigEndian.PutUint32(data[offset:offset+4], req.PartitionID)
	offset += 4
	binary.BigEndian.PutUint64(data[offset:offset+8], req.Offset)
	offset += 8
	binary.BigEndian.PutUint32(data[offset:offset+4], req.MaxMessages)
	return data, nil
}

// DecodeConsumeRequest decodes a consume request and rejects malformed input.
func DecodeConsumeRequest(data []byte) (ConsumeRequest, error) {
	if len(data) < 2 {
		return ConsumeRequest{}, fmt.Errorf("%w: consume request has no topic length", ErrInvalidRequest)
	}

	topicLength := int(binary.BigEndian.Uint16(data[:2]))
	if topicLength == 0 || topicLength > 249 {
		return ConsumeRequest{}, fmt.Errorf("%w: invalid topic length %d", ErrInvalidRequest, topicLength)
	}

	expectedSize := 2 + topicLength + 4 + 8 + 4
	if len(data) != expectedSize {
		return ConsumeRequest{}, fmt.Errorf("%w: consume request length is %d, expected %d", ErrInvalidRequest, len(data), expectedSize)
	}

	topicName := string(data[2 : 2+topicLength])
	if err := validateConsumeTopicName(topicName); err != nil {
		return ConsumeRequest{}, err
	}

	offset := 2 + topicLength
	req := ConsumeRequest{
		Topic:       topicName,
		PartitionID: binary.BigEndian.Uint32(data[offset : offset+4]),
	}
	offset += 4
	req.Offset = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	req.MaxMessages = binary.BigEndian.Uint32(data[offset : offset+4])

	if req.MaxMessages == 0 || req.MaxMessages > MaxConsumeMessages {
		return ConsumeRequest{}, fmt.Errorf("%w: must be between 1 and %d", ErrInvalidFetchLimit, MaxConsumeMessages)
	}

	return req, nil
}

// EncodeConsumeResponse encodes a bounded set of consumed records.
//
// Wire format:
// [StartOffset: 8][LogEndOffset: 8][NextOffset: 8][RecordCount: 4]
// Then, for each record:
// [Offset: 8][Timestamp: 8][ValueLength: 4][Value]
func EncodeConsumeResponse(resp ConsumeResponse) ([]byte, error) {
	if err := validateConsumeResponse(resp); err != nil {
		return nil, err
	}

	totalSize := consumeResponseHeaderSize
	for _, record := range resp.Records {
		if len(record.Value) > MaxMessageSize {
			return nil, fmt.Errorf("%w: consumed record exceeds maximum message size", ErrMessageTooLarge)
		}
		if totalSize > MaxBatchSize-consumeRecordHeaderSize-len(record.Value) {
			return nil, fmt.Errorf("%w: consume response exceeds maximum size", ErrMessageTooLarge)
		}
		totalSize += consumeRecordHeaderSize + len(record.Value)
	}

	data := make([]byte, totalSize)
	binary.BigEndian.PutUint64(data[0:8], resp.StartOffset)
	binary.BigEndian.PutUint64(data[8:16], resp.LogEndOffset)
	binary.BigEndian.PutUint64(data[16:24], resp.NextOffset)
	binary.BigEndian.PutUint32(data[24:28], uint32(len(resp.Records)))

	offset := consumeResponseHeaderSize
	for _, record := range resp.Records {
		binary.BigEndian.PutUint64(data[offset:offset+8], record.Offset)
		offset += 8
		binary.BigEndian.PutUint64(data[offset:offset+8], uint64(record.Timestamp))
		offset += 8
		binary.BigEndian.PutUint32(data[offset:offset+4], uint32(len(record.Value)))
		offset += 4
		copy(data[offset:], record.Value)
		offset += len(record.Value)
	}

	return data, nil
}

// DecodeConsumeResponse decodes and validates a consume response.
func DecodeConsumeResponse(data []byte) (ConsumeResponse, error) {
	if len(data) < consumeResponseHeaderSize {
		return ConsumeResponse{}, fmt.Errorf("%w: consume response header is incomplete", ErrInvalidRequest)
	}
	if len(data) > MaxBatchSize {
		return ConsumeResponse{}, fmt.Errorf("%w: consume response exceeds maximum size", ErrMessageTooLarge)
	}

	resp := ConsumeResponse{
		StartOffset:  binary.BigEndian.Uint64(data[0:8]),
		LogEndOffset: binary.BigEndian.Uint64(data[8:16]),
		NextOffset:   binary.BigEndian.Uint64(data[16:24]),
	}
	count := binary.BigEndian.Uint32(data[24:28])
	if count > MaxConsumeMessages {
		return ConsumeResponse{}, fmt.Errorf("%w: response contains too many records", ErrInvalidRequest)
	}

	remaining := len(data) - consumeResponseHeaderSize
	if uint64(count)*consumeRecordHeaderSize > uint64(remaining) {
		return ConsumeResponse{}, fmt.Errorf("%w: not enough bytes for declared records", ErrInvalidRequest)
	}

	resp.Records = make([]ConsumeRecord, 0, int(count))
	offset := consumeResponseHeaderSize
	for i := uint32(0); i < count; i++ {
		if len(data)-offset < consumeRecordHeaderSize {
			return ConsumeResponse{}, fmt.Errorf("%w: record %d header is incomplete", ErrInvalidRequest, i)
		}

		record := ConsumeRecord{
			Offset:    binary.BigEndian.Uint64(data[offset : offset+8]),
			Timestamp: int64(binary.BigEndian.Uint64(data[offset+8 : offset+16])),
		}
		valueLength := binary.BigEndian.Uint32(data[offset+16 : offset+20])
		offset += consumeRecordHeaderSize

		if valueLength > MaxMessageSize {
			return ConsumeResponse{}, fmt.Errorf("%w: record %d value is too large", ErrMessageTooLarge, i)
		}
		if uint64(valueLength) > uint64(len(data)-offset) {
			return ConsumeResponse{}, fmt.Errorf("%w: record %d value is truncated", ErrInvalidRequest, i)
		}

		end := offset + int(valueLength)
		record.Value = append([]byte(nil), data[offset:end]...)
		resp.Records = append(resp.Records, record)
		offset = end
	}

	if offset != len(data) {
		return ConsumeResponse{}, fmt.Errorf("%w: consume response has %d trailing bytes", ErrInvalidRequest, len(data)-offset)
	}
	if err := validateConsumeResponse(resp); err != nil {
		return ConsumeResponse{}, err
	}

	return resp, nil
}

func validateConsumeResponse(resp ConsumeResponse) error {
	if resp.StartOffset > resp.LogEndOffset {
		return fmt.Errorf("%w: start offset exceeds log end offset", ErrInvalidRequest)
	}
	if resp.NextOffset < resp.StartOffset || resp.NextOffset > resp.LogEndOffset {
		return fmt.Errorf("%w: next offset is outside the retained log range", ErrInvalidRequest)
	}
	if len(resp.Records) > int(MaxConsumeMessages) {
		return fmt.Errorf("%w: too many records", ErrInvalidRequest)
	}

	var previous uint64
	for i, record := range resp.Records {
		if record.Offset < resp.StartOffset || record.Offset >= resp.LogEndOffset {
			return fmt.Errorf("%w: record %d offset is outside the log range", ErrInvalidRequest, i)
		}
		if i > 0 && record.Offset <= previous {
			return fmt.Errorf("%w: record offsets are not strictly increasing", ErrInvalidRequest)
		}
		previous = record.Offset
	}
	if len(resp.Records) > 0 && resp.NextOffset <= previous {
		return fmt.Errorf("%w: next offset does not follow returned records", ErrInvalidRequest)
	}
	return nil
}

func validateConsumeTopicName(name string) error {
	if len(name) == 0 || len(name) > 249 || name == "." || name == ".." {
		return fmt.Errorf("%w: invalid topic name", ErrInvalidRequest)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		valid := (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'
		if !valid {
			return fmt.Errorf("%w: invalid topic name character %q", ErrInvalidRequest, c)
		}
	}
	return nil
}
