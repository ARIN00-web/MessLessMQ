package consumer

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ARIN00-web/messlessmq/internal/storage"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

const MaxFetchMessages uint32 = 500

var (
	ErrNoPartitions         = errors.New("consumer requires at least one partition")
	ErrNilPartition         = errors.New("consumer cannot be assigned a nil partition")
	ErrDuplicatePartition   = errors.New("consumer cannot be assigned the same partition twice")
	ErrPartitionNotAssigned = errors.New("partition is not assigned to consumer")
	ErrInvalidFetchLimit    = errors.New("invalid fetch limit")
	ErrOffsetBeforeStart    = errors.New("requested offset is before the earliest retained offset")
	ErrOffsetOutOfRange     = errors.New("requested offset is beyond the log end offset")
)

type partitionKey struct {
	topicName   string
	partitionID uint32
}

// Record is one message returned by a partition fetch. The current storage
// record format persists payload and timestamp; producer keys are not yet
// stored as independent record fields.
type Record struct {
	Topic     string
	Partition uint32
	Offset    uint64
	Timestamp int64
	Value     []byte
}

// FetchResult describes one bounded read from a partition.
type FetchResult struct {
	Topic        string
	Partition    uint32
	StartOffset  uint64
	LogEndOffset uint64
	NextOffset   uint64
	Records      []Record
}

// Consumer is an individual logical reader with explicit partition
// assignments and in-memory next-read positions. Positions are not durable
// commits; persistent commits belong to Phase 11.
type Consumer struct {
	mu         sync.Mutex
	partitions []*topic.Partition
	assigned   map[partitionKey]*topic.Partition
	positions  map[partitionKey]uint64
}

// New creates a consumer with an explicit set of assigned partitions.
// Initial positions start at the earliest retained offset when a partition
// log is open, or at zero when the log has not yet been opened.
func New(partitions ...*topic.Partition) (*Consumer, error) {
	if len(partitions) == 0 {
		return nil, ErrNoPartitions
	}

	c := &Consumer{
		partitions: make([]*topic.Partition, 0, len(partitions)),
		assigned:   make(map[partitionKey]*topic.Partition, len(partitions)),
		positions:  make(map[partitionKey]uint64, len(partitions)),
	}

	for i, partition := range partitions {
		if partition == nil {
			return nil, fmt.Errorf("%w: index %d", ErrNilPartition, i)
		}

		key := partitionKey{
			topicName:   partition.TopicName(),
			partitionID: partition.ID(),
		}
		if _, exists := c.assigned[key]; exists {
			return nil, fmt.Errorf(
				"%w: topic=%q partition=%d",
				ErrDuplicatePartition,
				key.topicName,
				key.partitionID,
			)
		}

		position := uint64(0)
		startOffset, err := partition.StartOffset()
		if err == nil {
			position = startOffset
		} else if !errors.Is(err, topic.ErrPartitionLogNotOpen) {
			return nil, fmt.Errorf(
				"initialize position for topic %q partition %d: %w",
				key.topicName,
				key.partitionID,
				err,
			)
		}

		c.partitions = append(c.partitions, partition)
		c.assigned[key] = partition
		c.positions[key] = position
	}

	return c, nil
}

// PartitionCount returns the number of assigned partitions.
func (c *Consumer) PartitionCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.partitions)
}

// Partitions returns a copy of the assigned partition collection.
func (c *Consumer) Partitions() []*topic.Partition {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*topic.Partition(nil), c.partitions...)
}

// HasPartition reports whether the consumer owns a logical topic-partition.
func (c *Consumer) HasPartition(topicName string, partitionID uint32) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, exists := c.assigned[partitionKey{topicName: topicName, partitionID: partitionID}]
	return exists
}

// GetPartition returns an assigned partition by logical identity.
func (c *Consumer) GetPartition(topicName string, partitionID uint32) (*topic.Partition, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: topic=%q partition=%d", ErrPartitionNotAssigned, topicName, partitionID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := partitionKey{topicName: topicName, partitionID: partitionID}
	partition, exists := c.assigned[key]
	if !exists {
		return nil, fmt.Errorf("%w: topic=%q partition=%d", ErrPartitionNotAssigned, topicName, partitionID)
	}
	return partition, nil
}

// Fetch reads up to maxMessages records from the consumer's current position
// and advances that in-memory position to the next unread offset.
func (c *Consumer) Fetch(topicName string, partitionID uint32, maxMessages uint32) (FetchResult, error) {
	if err := validateFetchLimit(maxMessages); err != nil {
		return FetchResult{}, err
	}
	if c == nil {
		return FetchResult{}, fmt.Errorf("%w: nil consumer", ErrPartitionNotAssigned)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := partitionKey{topicName: topicName, partitionID: partitionID}
	partition, exists := c.assigned[key]
	if !exists {
		return FetchResult{}, fmt.Errorf("%w: topic=%q partition=%d", ErrPartitionNotAssigned, topicName, partitionID)
	}

	result, err := fetchFromPartition(partition, c.positions[key], maxMessages)
	if err != nil {
		return FetchResult{}, err
	}
	c.positions[key] = result.NextOffset
	return result, nil
}

// Position returns the next offset this consumer will fetch for a partition.
func (c *Consumer) Position(topicName string, partitionID uint32) (uint64, error) {
	if c == nil {
		return 0, ErrPartitionNotAssigned
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := partitionKey{topicName: topicName, partitionID: partitionID}
	position, exists := c.positions[key]
	if !exists {
		return 0, fmt.Errorf("%w: topic=%q partition=%d", ErrPartitionNotAssigned, topicName, partitionID)
	}
	return position, nil
}

// SetPosition moves the next-read position after validating it against the
// partition's currently retained range [StartOffset, LogEndOffset].
func (c *Consumer) SetPosition(topicName string, partitionID uint32, offset uint64) error {
	if c == nil {
		return ErrPartitionNotAssigned
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := partitionKey{topicName: topicName, partitionID: partitionID}
	partition, exists := c.assigned[key]
	if !exists {
		return fmt.Errorf("%w: topic=%q partition=%d", ErrPartitionNotAssigned, topicName, partitionID)
	}
	if err := validatePosition(partition, offset); err != nil {
		return err
	}
	c.positions[key] = offset
	return nil
}

// ResetToEarliest moves the consumer to the earliest offset still retained.
func (c *Consumer) ResetToEarliest(topicName string, partitionID uint32) error {
	if c == nil {
		return ErrPartitionNotAssigned
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := partitionKey{topicName: topicName, partitionID: partitionID}
	partition, exists := c.assigned[key]
	if !exists {
		return fmt.Errorf("%w: topic=%q partition=%d", ErrPartitionNotAssigned, topicName, partitionID)
	}
	start, err := partition.StartOffset()
	if err != nil {
		return fmt.Errorf("read earliest offset: %w", err)
	}
	c.positions[key] = start
	return nil
}

// ResetToLatest moves the consumer to the current log end, skipping all
// records that exist at the time of the operation.
func (c *Consumer) ResetToLatest(topicName string, partitionID uint32) error {
	if c == nil {
		return ErrPartitionNotAssigned
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := partitionKey{topicName: topicName, partitionID: partitionID}
	partition, exists := c.assigned[key]
	if !exists {
		return fmt.Errorf("%w: topic=%q partition=%d", ErrPartitionNotAssigned, topicName, partitionID)
	}
	end, err := partition.NextOffset()
	if err != nil {
		return fmt.Errorf("read log end offset: %w", err)
	}
	c.positions[key] = end
	return nil
}

// Replay resets the consumer to fromOffset, reads up to maxMessages records,
// and advances its in-memory position to the next unread offset.
func (c *Consumer) Replay(topicName string, partitionID uint32, fromOffset uint64, maxMessages uint32) (FetchResult, error) {
	if err := validateFetchLimit(maxMessages); err != nil {
		return FetchResult{}, err
	}
	if c == nil {
		return FetchResult{}, ErrPartitionNotAssigned
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := partitionKey{topicName: topicName, partitionID: partitionID}
	partition, exists := c.assigned[key]
	if !exists {
		return FetchResult{}, fmt.Errorf("%w: topic=%q partition=%d", ErrPartitionNotAssigned, topicName, partitionID)
	}
	result, err := fetchFromPartition(partition, fromOffset, maxMessages)
	if err != nil {
		return FetchResult{}, err
	}
	c.positions[key] = result.NextOffset
	return result, nil
}

func validateFetchLimit(maxMessages uint32) error {
	if maxMessages == 0 || maxMessages > MaxFetchMessages {
		return fmt.Errorf("%w: must be between 1 and %d", ErrInvalidFetchLimit, MaxFetchMessages)
	}
	return nil
}

func validatePosition(partition *topic.Partition, offset uint64) error {
	start, end, err := partition.OffsetRange()
	if err != nil {
		return fmt.Errorf("read partition offset range: %w", err)
	}
	if offset < start {
		return fmt.Errorf("%w: requested=%d earliest=%d", ErrOffsetBeforeStart, offset, start)
	}
	if offset > end {
		return fmt.Errorf("%w: requested=%d log_end=%d", ErrOffsetOutOfRange, offset, end)
	}
	return nil
}

func fetchFromPartition(partition *topic.Partition, offset uint64, maxMessages uint32) (FetchResult, error) {
	start, end, err := partition.OffsetRange()
	if err != nil {
		return FetchResult{}, fmt.Errorf("read partition offset range: %w", err)
	}

	if offset < start {
		return FetchResult{}, fmt.Errorf("%w: requested=%d earliest=%d", ErrOffsetBeforeStart, offset, start)
	}
	if offset > end {
		return FetchResult{}, fmt.Errorf("%w: requested=%d log_end=%d", ErrOffsetOutOfRange, offset, end)
	}

	result := FetchResult{
		Topic:        partition.TopicName(),
		Partition:    partition.ID(),
		StartOffset:  start,
		LogEndOffset: end,
		NextOffset:   offset,
		Records:      make([]Record, 0, maxMessages),
	}

	cursor := offset
	for cursor < end && uint32(len(result.Records)) < maxMessages {
		value, timestamp, err := partition.ReadWithTimestamp(cursor)
		if err != nil {
			// Retention can advance the earliest offset after the range snapshot
			// above. Re-check the range so callers receive the precise offset
			// error instead of an opaque storage lookup failure.
			if errors.Is(err, storage.ErrOffsetNotFound) {
				newStart, newEnd, rangeErr := partition.OffsetRange()
				if rangeErr == nil {
					if cursor < newStart {
						return FetchResult{}, fmt.Errorf("%w: requested=%d earliest=%d", ErrOffsetBeforeStart, cursor, newStart)
					}
					if cursor > newEnd {
						return FetchResult{}, fmt.Errorf("%w: requested=%d log_end=%d", ErrOffsetOutOfRange, cursor, newEnd)
					}
				}
			}
			return FetchResult{}, fmt.Errorf("read topic %q partition %d offset %d: %w", partition.TopicName(), partition.ID(), cursor, err)
		}
		result.Records = append(result.Records, Record{
			Topic:     partition.TopicName(),
			Partition: partition.ID(),
			Offset:    cursor,
			Timestamp: timestamp,
			Value:     value,
		})
		cursor++
	}

	result.NextOffset = cursor
	return result, nil
}
