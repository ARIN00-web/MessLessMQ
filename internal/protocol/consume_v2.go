package protocol

import (
	"encoding/binary"
	"fmt"
)

const consumeV2RecordHeaderSize = 8 + 8 + 4 + 4

// EncodeConsumeResponseV2 adds the original message key to every returned
// record while keeping the version-1 response layout unchanged.
func EncodeConsumeResponseV2(resp ConsumeResponse) ([]byte, error) {
	if err := validateConsumeResponse(resp); err != nil {
		return nil, err
	}
	total := consumeResponseHeaderSize
	for _, record := range resp.Records {
		if len(record.Key) > MaxMessageSize || len(record.Value) > MaxMessageSize {
			return nil, ErrMessageTooLarge
		}
		if total > MaxBatchSize-consumeV2RecordHeaderSize-len(record.Key)-len(record.Value) {
			return nil, ErrMessageTooLarge
		}
		total += consumeV2RecordHeaderSize + len(record.Key) + len(record.Value)
	}
	data := make([]byte, total)
	binary.BigEndian.PutUint64(data[0:8], resp.StartOffset)
	binary.BigEndian.PutUint64(data[8:16], resp.LogEndOffset)
	binary.BigEndian.PutUint64(data[16:24], resp.NextOffset)
	binary.BigEndian.PutUint32(data[24:28], uint32(len(resp.Records)))
	cursor := consumeResponseHeaderSize
	for _, record := range resp.Records {
		binary.BigEndian.PutUint64(data[cursor:cursor+8], record.Offset)
		cursor += 8
		binary.BigEndian.PutUint64(data[cursor:cursor+8], uint64(record.Timestamp))
		cursor += 8
		keyLength := int32(len(record.Key))
		if record.Key == nil {
			keyLength = -1
		}
		binary.BigEndian.PutUint32(data[cursor:cursor+4], uint32(keyLength))
		cursor += 4
		binary.BigEndian.PutUint32(data[cursor:cursor+4], uint32(len(record.Value)))
		cursor += 4
		if record.Key != nil {
			copy(data[cursor:], record.Key)
			cursor += len(record.Key)
		}
		copy(data[cursor:], record.Value)
		cursor += len(record.Value)
	}
	return data, nil
}

func DecodeConsumeResponseV2(data []byte) (ConsumeResponse, error) {
	if len(data) < consumeResponseHeaderSize || len(data) > MaxBatchSize {
		return ConsumeResponse{}, ErrInvalidRequest
	}
	response := ConsumeResponse{
		StartOffset:  binary.BigEndian.Uint64(data[0:8]),
		LogEndOffset: binary.BigEndian.Uint64(data[8:16]),
		NextOffset:   binary.BigEndian.Uint64(data[16:24]),
	}
	count := binary.BigEndian.Uint32(data[24:28])
	if count > MaxConsumeMessages || uint64(count)*consumeV2RecordHeaderSize > uint64(len(data)-consumeResponseHeaderSize) {
		return ConsumeResponse{}, ErrInvalidRequest
	}
	response.Records = make([]ConsumeRecord, 0, count)
	cursor := consumeResponseHeaderSize
	for i := uint32(0); i < count; i++ {
		if len(data)-cursor < consumeV2RecordHeaderSize {
			return ConsumeResponse{}, fmt.Errorf("%w: record %d header is incomplete", ErrInvalidRequest, i)
		}
		record := ConsumeRecord{Offset: binary.BigEndian.Uint64(data[cursor : cursor+8]), Timestamp: int64(binary.BigEndian.Uint64(data[cursor+8 : cursor+16]))}
		keyLength := int32(binary.BigEndian.Uint32(data[cursor+16 : cursor+20]))
		valueLength := binary.BigEndian.Uint32(data[cursor+20 : cursor+24])
		cursor += consumeV2RecordHeaderSize
		if keyLength < -1 || keyLength > MaxMessageSize || valueLength > MaxMessageSize {
			return ConsumeResponse{}, ErrMessageTooLarge
		}
		keyBytes := uint64(0)
		if keyLength >= 0 {
			keyBytes = uint64(keyLength)
		}
		if keyBytes+uint64(valueLength) > uint64(len(data)-cursor) {
			return ConsumeResponse{}, ErrInvalidRequest
		}
		if keyLength >= 0 {
			record.Key = append([]byte{}, data[cursor:cursor+int(keyBytes)]...)
			cursor += int(keyBytes)
		}
		end := cursor + int(valueLength)
		record.Value = append([]byte(nil), data[cursor:end]...)
		cursor = end
		response.Records = append(response.Records, record)
	}
	if cursor != len(data) {
		return ConsumeResponse{}, fmt.Errorf("%w: consume v2 response has trailing bytes", ErrInvalidRequest)
	}
	if err := validateConsumeResponse(response); err != nil {
		return ConsumeResponse{}, err
	}
	return response, nil
}
