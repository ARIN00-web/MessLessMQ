package topic

import (
	"errors"
	"fmt"
	"sync"
)

const MaxTopicNameLength = 249

var ErrInvalidTopicName = errors.New("invalid topic name")

// Topic represents a named logical message stream.
//
// Its name and initial partition collection are immutable after creation.
// Physical storage ownership will be added in a later phase.
type Topic struct {
	name       string
	partitions []*Partition

	// Protects changes to the partition collection.
	partitionMu sync.RWMutex

	// Serializes opening, closing, and expanding partition logs.
	logMu sync.Mutex

	// Storage configuration for partitions added while logs are open.
	logRootDir      string
	maxSegmentBytes int64
	logsOpen        bool
}

// Name returns the topic's immutable name.
func (t *Topic) Name() string {
	if t == nil {
		return ""
	}
	return t.name
}

// ValidateTopicName validates a topic name before it enters the registry.
// Names are 1-249 ASCII characters and may contain letters, digits, '.', '_',
// and '-'. The special names "." and ".." are rejected.
func ValidateTopicName(name string) error {
	if len(name) == 0 ||
		len(name) > MaxTopicNameLength ||
		name == "." ||
		name == ".." {
		return fmt.Errorf(
			"%w: expected 1-%d allowed characters and not '.' or '..'",
			ErrInvalidTopicName,
			MaxTopicNameLength,
		)
	}

	for i := 0; i < len(name); i++ {
		c := name[i]

		valid := (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '.' ||
			c == '_' ||
			c == '-'

		if !valid {
			return fmt.Errorf(
				"%w: unsupported character %q",
				ErrInvalidTopicName,
				c,
			)
		}
	}

	return nil
}
