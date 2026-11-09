package broker

import (
	"fmt"
)

// PartitionAppendResult identifies where a message was appended.
type PartitionAppendResult struct {
	PartitionID uint32
	Offset      uint64
}

// Append assigns a message to a partition and appends it to that partition's
// log.
//
// For a non-nil key, the existing hash-based partitioner selects the
// destination. For a nil key, round-robin assignment is used.
//
// Sequential appends using the same key go to the same partition, provided
// the partition count and hash algorithm remain unchanged.
//
// Concurrent appends are ordered by the storage log's append serialization,
// not by goroutine start time.
func (a *PartitionAssignment) Append(
	key []byte,
	value []byte,
) (PartitionAppendResult, error) {
	partition, err := a.Assign(key)
	if err != nil {
		return PartitionAppendResult{}, err
	}

	offset, err := partition.Append(value)
	if err != nil {
		return PartitionAppendResult{}, fmt.Errorf(
			"append to topic %q partition %d: %w",
			partition.TopicName(),
			partition.ID(),
			err,
		)
	}

	return PartitionAppendResult{
		PartitionID: partition.ID(),
		Offset:      offset,
	}, nil
}
