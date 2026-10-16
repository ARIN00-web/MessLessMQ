package broker

import (
	"errors"
	"hash/fnv"
	"sync/atomic"
)

var ErrInvalidPartitionCount = errors.New("partition count must be positive")

type Partitioner struct {
	partitionCount uint32
	next           atomic.Uint64
}

func NewPartitioner(partitionCount uint32) (*Partitioner, error) {
	if partitionCount == 0 {
		return nil, ErrInvalidPartitionCount
	}

	return &Partitioner{
		partitionCount: partitionCount,
	}, nil
}

func (p *Partitioner) Partition(key []byte) (uint32, error) {
	if p == nil || p.partitionCount == 0 {
		return 0, ErrInvalidPartitionCount
	}

	// A missing key uses round-robin routing.
	if key == nil {
		n := p.next.Add(1) - 1
		return uint32(n % uint64(p.partitionCount)), nil
	}

	// A present key uses a deterministic hash.
	hash := fnv.New32a()
	_, _ = hash.Write(key)

	return hash.Sum32() % p.partitionCount, nil
}
