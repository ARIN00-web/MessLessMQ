package protocol

import (
	"encoding/binary"
	"fmt"
)

const producePayloadHeaderSize = 6

type ProduceRequest struct {
	AckMode AckMode
	Batch   MessageBatch
}

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
		return nil, fmt.Errorf("produce payload exceeds maximum size")
	}

	data := make([]byte, totalSize)

	binary.BigEndian.PutUint16(data[0:2], uint16(int16(req.AckMode)))
	binary.BigEndian.PutUint32(data[2:6], uint32(len(encodedBatch)))
	copy(data[6:], encodedBatch)

	return data, nil
}

func DecodeProduceRequest(data []byte) (ProduceRequest, error) {
	if len(data) < producePayloadHeaderSize {
		return ProduceRequest{}, fmt.Errorf("produce payload header is incomplete")
	}

	ackMode := AckMode(int16(binary.BigEndian.Uint16(data[0:2])))
	if err := ackMode.Validate(); err != nil {
		return ProduceRequest{}, err
	}

	batchLength := binary.BigEndian.Uint32(data[2:6])
	remaining := len(data) - producePayloadHeaderSize

	if batchLength > MaxBatchSize {
		return ProduceRequest{}, fmt.Errorf("produce batch exceeds maximum size")
	}

	if int(batchLength) != remaining {
		return ProduceRequest{}, fmt.Errorf(
			"invalid batch length: declared %d bytes, got %d",
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
