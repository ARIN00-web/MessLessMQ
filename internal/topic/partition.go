package topic

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

var (
	ErrInvalidPartitionCount     = errors.New("partition count must be positive")
	ErrPartitionNotFound         = errors.New("partition not found")
	ErrPartitionLogNotOpen       = errors.New("partition log is not open")
	ErrPartitionLogAlreadyOpen   = errors.New("partition log is already open")
	ErrInvalidPartitionLogConfig = errors.New("invalid partition log configuration")
)

// Partition represents an independently ordered subdivision of a topic.
//
// Each partition owns one storage.Log. Its physical log lifecycle is managed
// through the owning Topic.
type Partition struct {
	topicName string
	id        uint32

	mu  sync.RWMutex
	log *storage.Log
}

// ID returns the partition's zero-based ID.
func (p *Partition) ID() uint32 {
	if p == nil {
		return 0
	}
	return p.id
}

// TopicName returns the name of the partition's owning topic.
func (p *Partition) TopicName() string {
	if p == nil {
		return ""
	}
	return p.topicName
}

// newTopic creates a topic with a fixed initial set of partitions.
// Partition IDs are contiguous and start at zero.
func newTopic(name string, partitionCount uint32) *Topic {
	partitions := make([]*Partition, int(partitionCount))

	for id := range partitions {
		partitions[id] = &Partition{
			topicName: name,
			id:        uint32(id),
		}
	}

	return &Topic{
		name:       name,
		partitions: partitions,
	}
}

// PartitionCount returns the current number of partitions.
func (t *Topic) PartitionCount() uint32 {
	if t == nil {
		return 0
	}

	t.partitionMu.RLock()
	defer t.partitionMu.RUnlock()

	return uint32(len(t.partitions))
}

// Partitions returns a copy of the topic's partition collection.
func (t *Topic) Partitions() []*Partition {
	if t == nil {
		return nil
	}

	t.partitionMu.RLock()
	defer t.partitionMu.RUnlock()

	return append([]*Partition(nil), t.partitions...)
}

// Partition returns a partition by its zero-based ID.
func (t *Topic) Partition(id uint32) (*Partition, error) {
	if t == nil {
		return nil, fmt.Errorf(
			"%w: partition=%d",
			ErrPartitionNotFound,
			id,
		)
	}

	t.partitionMu.RLock()
	defer t.partitionMu.RUnlock()

	if uint64(id) >= uint64(len(t.partitions)) {
		return nil, fmt.Errorf(
			"%w: topic=%q partition=%d",
			ErrPartitionNotFound,
			t.name,
			id,
		)
	}

	return t.partitions[id], nil
}
