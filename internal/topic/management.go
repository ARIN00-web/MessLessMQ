package topic

import (
	"errors"
	"fmt"
)

var ErrPartitionCountOverflow = errors.New("partition count exceeds supported range")

// AddPartitions adds the requested number of partitions to the topic.
//
// New partition IDs continue from the current partition count. Existing IDs
// are never changed or reused.
//
// If the topic's logs are already open, new partition logs are opened before
// the new partitions become visible. If opening any new log fails, previously
// opened new logs are closed and the partition collection remains unchanged.
func (t *Topic) AddPartitions(additional uint32) error {
	if t == nil {
		return ErrInvalidPartitionCount
	}

	if err := ValidateTopicName(t.name); err != nil {
		return err
	}

	if additional == 0 {
		return ErrInvalidPartitionCount
	}

	// Lock order is always logMu followed by partitionMu.
	t.logMu.Lock()
	defer t.logMu.Unlock()

	t.partitionMu.Lock()
	defer t.partitionMu.Unlock()

	currentCount := uint64(len(t.partitions))
	newCount := currentCount + uint64(additional)

	maxPartitionCount := uint64(^uint32(0))
	maxInt := uint64(^uint(0) >> 1)

	if newCount > maxPartitionCount || newCount > maxInt {
		return ErrPartitionCountOverflow
	}

	added := make([]*Partition, int(additional))

	for i := range added {
		id := currentCount + uint64(i)

		added[i] = &Partition{
			topicName: t.name,
			id:        uint32(id),
		}
	}

	// Prepare storage before publishing the new partitions.
	if t.logsOpen {
		opened := make([]*Partition, 0, len(added))

		for _, partition := range added {
			dir := partitionLogDirectory(
				t.logRootDir,
				t.name,
				partition.id,
			)

			if err := partition.openLog(dir, t.maxSegmentBytes); err != nil {
				openErr := fmt.Errorf(
					"open new partition %d for topic %q: %w",
					partition.id,
					t.name,
					err,
				)

				return errors.Join(
					openErr,
					closeOpenedPartitions(opened),
				)
			}

			opened = append(opened, partition)
		}
	}

	// All required preparation succeeded. Publish the new partitions.
	t.partitions = append(t.partitions, added...)

	return nil
}
