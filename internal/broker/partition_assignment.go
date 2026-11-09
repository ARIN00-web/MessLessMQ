package broker

import (
	"errors"
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/topic"
)

var ErrInvalidPartitionAssignment = errors.New("invalid partition assignment")

// PartitionAssignment connects a topic's partition collection to a
// partitioning strategy.
//
// Each topic should have its own PartitionAssignment instance so that
// round-robin routing maintains an independent sequence per topic.
type PartitionAssignment struct {
	topic       *topic.Topic
	partitioner *Partitioner
}

// NewPartitionAssignment creates a partition assignment component for a topic.
//
// The partitioner's partition count is derived from the topic, ensuring
// that routing cannot target a partition outside the topic's collection.
func NewPartitionAssignment(t *topic.Topic) (*PartitionAssignment, error) {
	if t == nil {
		return nil, ErrInvalidPartitionAssignment
	}

	partitionCount := t.PartitionCount()
	if partitionCount == 0 {
		return nil, ErrInvalidPartitionCount
	}

	partitioner, err := NewPartitioner(partitionCount)
	if err != nil {
		return nil, fmt.Errorf("create topic partitioner: %w", err)
	}

	return &PartitionAssignment{
		topic:       t,
		partitioner: partitioner,
	}, nil
}

// Assign selects and returns the actual partition for a message key.
//
// A nil key uses round-robin assignment. A non-nil key uses deterministic
// hash-based assignment through the existing Partitioner.
func (a *PartitionAssignment) Assign(key []byte) (*topic.Partition, error) {
	if a == nil || a.topic == nil || a.partitioner == nil {
		return nil, ErrInvalidPartitionAssignment
	}

	partitionID, err := a.partitioner.Partition(key)
	if err != nil {
		return nil, fmt.Errorf("calculate partition: %w", err)
	}

	partition, err := a.topic.Partition(partitionID)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve topic %q partition %d: %w",
			a.topic.Name(),
			partitionID,
			err,
		)
	}

	return partition, nil
}
