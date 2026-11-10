package replication

import (
	"encoding/binary"
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

const (
	replicationBatchHeaderSize  = 22
	replicationRecordHeaderSize = 20
)

// EncodeReplicationBatch encodes a versioned replication batch using network
// byte order. The batch is transport-neutral; Phase 16.5 will carry it over
// the inter-broker connection.
//
// Layout:
//
//	version(2), topic length(2), leader ID length(2), partition ID(4),
//	leader epoch(8), record count(4), topic, leader ID, then records.
//
// Each record contains offset(8), timestamp(8), value length(4), value.
func EncodeReplicationBatch(batch ReplicationBatch) ([]byte, error) {
	if batch.Version == 0 {
		batch.Version = ProtocolVersion
	}
	if err := validateReplicationBatch(batch); err != nil {
		return nil, err
	}
	if len(batch.Topic) > 0xffff || len(batch.LeaderID) > 0xffff {
		return nil, ErrInvalidReplicationBatch
	}

	total := replicationBatchHeaderSize + len(batch.Topic) + len(batch.LeaderID)
	for _, record := range batch.Records {
		if len(record.Value) > storage.MaxRecordSize {
			return nil, fmt.Errorf("record payload exceeds storage maximum: %w", ErrInvalidReplicationBatch)
		}
		total += replicationRecordHeaderSize + len(record.Value)
		if total > MaxReplicationFrameBytes {
			return nil, ErrInvalidReplicationBatch
		}
	}

	data := make([]byte, total)
	binary.BigEndian.PutUint16(data[0:2], batch.Version)
	binary.BigEndian.PutUint16(data[2:4], uint16(len(batch.Topic)))
	binary.BigEndian.PutUint16(data[4:6], uint16(len(batch.LeaderID)))
	binary.BigEndian.PutUint32(data[6:10], batch.PartitionID)
	binary.BigEndian.PutUint64(data[10:18], batch.LeaderEpoch)
	binary.BigEndian.PutUint32(data[18:22], uint32(len(batch.Records)))

	cursor := replicationBatchHeaderSize
	copy(data[cursor:], batch.Topic)
	cursor += len(batch.Topic)
	copy(data[cursor:], string(batch.LeaderID))
	cursor += len(batch.LeaderID)

	for _, record := range batch.Records {
		binary.BigEndian.PutUint64(data[cursor:cursor+8], record.Offset)
		cursor += 8
		binary.BigEndian.PutUint64(data[cursor:cursor+8], uint64(record.Timestamp))
		cursor += 8
		binary.BigEndian.PutUint32(data[cursor:cursor+4], uint32(len(record.Value)))
		cursor += 4
		copy(data[cursor:], record.Value)
		cursor += len(record.Value)
	}
	return data, nil
}

// DecodeReplicationBatch validates and decodes one replication batch. It
// rejects unsupported versions, oversized values, non-contiguous offsets,
// truncated records, and trailing bytes.
func DecodeReplicationBatch(data []byte) (ReplicationBatch, error) {
	if len(data) < replicationBatchHeaderSize || len(data) > MaxReplicationFrameBytes {
		return ReplicationBatch{}, ErrInvalidReplicationBatch
	}

	batch := ReplicationBatch{
		Version:     binary.BigEndian.Uint16(data[0:2]),
		PartitionID: binary.BigEndian.Uint32(data[6:10]),
		LeaderEpoch: binary.BigEndian.Uint64(data[10:18]),
	}
	if batch.Version != ProtocolVersion {
		return ReplicationBatch{}, fmt.Errorf("version %d: %w", batch.Version, ErrInvalidReplicationBatch)
	}

	topicLength := int(binary.BigEndian.Uint16(data[2:4]))
	leaderLength := int(binary.BigEndian.Uint16(data[4:6]))
	recordCount := binary.BigEndian.Uint32(data[18:22])
	if topicLength == 0 || leaderLength == 0 || recordCount > MaxReplicationRecords {
		return ReplicationBatch{}, ErrInvalidReplicationBatch
	}

	cursor := replicationBatchHeaderSize
	if topicLength > len(data)-cursor {
		return ReplicationBatch{}, ErrInvalidReplicationBatch
	}
	batch.Topic = string(data[cursor : cursor+topicLength])
	cursor += topicLength
	if leaderLength > len(data)-cursor {
		return ReplicationBatch{}, ErrInvalidReplicationBatch
	}
	batch.LeaderID = ReplicaID(string(data[cursor : cursor+leaderLength]))
	cursor += leaderLength

	batch.Records = make([]Record, 0, int(recordCount))
	totalValues := 0
	for i := uint32(0); i < recordCount; i++ {
		if len(data)-cursor < replicationRecordHeaderSize {
			return ReplicationBatch{}, ErrInvalidReplicationBatch
		}
		offset := binary.BigEndian.Uint64(data[cursor : cursor+8])
		cursor += 8
		timestamp := int64(binary.BigEndian.Uint64(data[cursor : cursor+8]))
		cursor += 8
		valueLength := binary.BigEndian.Uint32(data[cursor : cursor+4])
		cursor += 4

		if valueLength > uint32(storage.MaxRecordSize) || uint64(valueLength) > uint64(len(data)-cursor) {
			return ReplicationBatch{}, ErrInvalidReplicationBatch
		}
		if i > 0 && offset != batch.Records[len(batch.Records)-1].Offset+1 {
			return ReplicationBatch{}, ErrInvalidReplicationBatch
		}
		totalValues += int(valueLength)
		if totalValues > MaxReplicationValueBytes {
			return ReplicationBatch{}, ErrInvalidReplicationBatch
		}
		value := append([]byte(nil), data[cursor:cursor+int(valueLength)]...)
		cursor += int(valueLength)
		batch.Records = append(batch.Records, Record{
			Offset: offset, Timestamp: timestamp, Value: value,
		})
	}
	if cursor != len(data) {
		return ReplicationBatch{}, ErrInvalidReplicationBatch
	}
	if err := validateReplicationBatch(batch); err != nil {
		return ReplicationBatch{}, err
	}
	return batch, nil
}
