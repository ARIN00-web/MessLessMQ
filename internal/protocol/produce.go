package protocol

import (
	"encoding/binary"
	"fmt"
)

const producePayloadHeaderSize = 6

const ProduceAPIVersion2 uint16 = 2

type ProduceRequest struct {
	AckMode AckMode
	Batch   MessageBatch
}

// EncodeProduceRequest encodes the acknowledgement mode and message batch.
//
// Wire format:
//
//	[AckMode: 2 bytes, signed]
//	[BatchLength: 4 bytes]
//	[EncodedBatch: BatchLength bytes]
func EncodeProduceRequest(req ProduceRequest) ([]byte, error) {
	if err := req.AckMode.Validate(); err != nil {
		return nil, err
	}

	encodedBatch, err := EncodeBatch(req.Batch)
	if err != nil {
		return nil, fmt.Errorf("encode produce batch: %w", err)
	}

	totalSize := producePayloadHeaderSize + len(encodedBatch)
	if totalSize > MaxBatchSize+producePayloadHeaderSize {
		return nil, fmt.Errorf(
			"%w: produce payload exceeds maximum size",
			ErrMessageTooLarge,
		)
	}

	data := make([]byte, totalSize)

	binary.BigEndian.PutUint16(data[0:2], uint16(int16(req.AckMode)))
	binary.BigEndian.PutUint32(data[2:6], uint32(len(encodedBatch)))
	copy(data[producePayloadHeaderSize:], encodedBatch)

	return data, nil
}

// DecodeProduceRequest decodes and validates a PRODUCE payload.
func DecodeProduceRequest(data []byte) (ProduceRequest, error) {
	if len(data) < producePayloadHeaderSize {
		return ProduceRequest{}, fmt.Errorf(
			"%w: produce payload header is incomplete",
			ErrInvalidRequest,
		)
	}

	ackMode := AckMode(int16(binary.BigEndian.Uint16(data[0:2])))
	if err := ackMode.Validate(); err != nil {
		return ProduceRequest{}, err
	}

	batchLength := binary.BigEndian.Uint32(data[2:6])
	remaining := len(data) - producePayloadHeaderSize

	if batchLength > MaxBatchSize {
		return ProduceRequest{}, fmt.Errorf(
			"%w: declared batch length %d exceeds maximum %d",
			ErrMessageTooLarge,
			batchLength,
			MaxBatchSize,
		)
	}

	if int(batchLength) != remaining {
		return ProduceRequest{}, fmt.Errorf(
			"%w: invalid batch length: declared %d bytes, got %d",
			ErrInvalidRequest,
			batchLength,
			remaining,
		)
	}

	batch, err := DecodeBatch(data[producePayloadHeaderSize:])
	if err != nil {
		return ProduceRequest{}, fmt.Errorf("decode produce batch: %w", err)
	}

	return ProduceRequest{
		AckMode: ackMode,
		Batch:   batch,
	}, nil
}

// ProduceRequestV2 makes the target partition explicit. It is used by
// cluster-aware clients so an unkeyed record can be routed to a selected
// partition leader instead of relying on each broker's local round-robin
// cursor. Its payload is [PartitionID:4][Produce v1 payload].
type ProduceRequestV2 struct {
	PartitionID uint32
	Request     ProduceRequest
}

func EncodeProduceRequestV2(req ProduceRequestV2) ([]byte, error) {
	v1, err := EncodeProduceRequest(req.Request)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 4+len(v1))
	binary.BigEndian.PutUint32(data[:4], req.PartitionID)
	copy(data[4:], v1)
	return data, nil
}

func DecodeProduceRequestV2(data []byte) (ProduceRequestV2, error) {
	if len(data) < 4 {
		return ProduceRequestV2{}, ErrInvalidRequest
	}
	request, err := DecodeProduceRequest(data[4:])
	if err != nil {
		return ProduceRequestV2{}, err
	}
	return ProduceRequestV2{PartitionID: binary.BigEndian.Uint32(data[:4]), Request: request}, nil
}
