package group

import (
	"errors"
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/consumer"
	"github.com/ARIN00-web/messlessmq/internal/offsets"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

// CommitOffset durably commits the next offset to consume for a partition
// currently owned by memberID in the supplied group generation.
//
// The committed value is the next unread offset, not the last record offset
// processed. A normal commit cannot move backwards; use ResetOffset for an
// intentional rewind.
func (c *Coordinator) CommitOffset(
	groupID string,
	memberID string,
	generation uint64,
	topicName string,
	partitionID uint32,
	nextOffset uint64,
) error {
	return c.CommitOffsetWithMetadata(
		groupID,
		memberID,
		generation,
		topicName,
		partitionID,
		nextOffset,
		"",
	)
}

// CommitOffsetWithMetadata commits an offset with optional application
// metadata, such as a processing batch identifier.
func (c *Coordinator) CommitOffsetWithMetadata(
	groupID string,
	memberID string,
	generation uint64,
	topicName string,
	partitionID uint32,
	nextOffset uint64,
	metadata string,
) error {
	if c == nil || c.offsetStore == nil {
		return ErrOffsetStoreNotConfigured
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	g, member, partition, err := c.commitAuthorityLocked(
		groupID,
		memberID,
		generation,
		topicName,
		partitionID,
	)
	if err != nil {
		return err
	}

	start, end, err := partition.OffsetRange()
	if err != nil {
		return fmt.Errorf("read partition offset range: %w", err)
	}
	if nextOffset < start || nextOffset > end {
		return fmt.Errorf(
			"%w: requested=%d available=[%d,%d]",
			ErrCommittedOffsetOutOfRange,
			nextOffset,
			start,
			end,
		)
	}

	position, err := member.reader.Position(topicName, partitionID)
	if err != nil {
		return fmt.Errorf("read consumer position: %w", err)
	}
	if nextOffset > position {
		return fmt.Errorf(
			"%w: requested=%d current=%d",
			ErrCommitAheadOfPosition,
			nextOffset,
			position,
		)
	}

	key := offsets.Key{
		GroupID:     g.id,
		Topic:       topicName,
		PartitionID: partitionID,
	}
	if metadata == "" {
		return c.offsetStore.Commit(key, nextOffset)
	}
	return c.offsetStore.CommitWithMetadata(key, nextOffset, metadata)
}

// ResetCommittedOffset is an administrative reset that does not require an
// active group member. It is useful when a group cannot rejoin because its
// saved offset has fallen behind the retained log start. If a member currently
// owns the partition, its in-memory cursor is updated as part of the operation.
func (c *Coordinator) ResetCommittedOffset(
	groupID string,
	topicName string,
	partitionID uint32,
	nextOffset uint64,
	reason string,
) error {
	if c == nil || c.registry == nil || c.offsetStore == nil {
		return ErrOffsetStoreNotConfigured
	}
	if err := validateIdentifier(groupID); err != nil {
		return fmt.Errorf("%w: %q", ErrInvalidGroupID, groupID)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	topicObject, err := c.registry.Get(topicName)
	if err != nil {
		return fmt.Errorf("%w: %q", ErrSubscribedTopicMissing, topicName)
	}
	partition, err := topicObject.Partition(partitionID)
	if err != nil {
		return fmt.Errorf("partition %s/%d: %w", topicName, partitionID, err)
	}
	start, end, err := partition.OffsetRange()
	if err != nil {
		return fmt.Errorf("read partition offset range: %w", err)
	}
	if nextOffset < start || nextOffset > end {
		return fmt.Errorf(
			"%w: requested=%d available=[%d,%d]",
			ErrCommittedOffsetOutOfRange,
			nextOffset,
			start,
			end,
		)
	}

	var activeReader *consumer.Consumer
	var previousPosition uint64
	if g := c.groups[groupID]; g != nil {
		for _, member := range g.members {
			if containsAssignment(member.assignments, topicName, partitionID) && member.reader != nil {
				activeReader = member.reader
				previousPosition, err = activeReader.Position(topicName, partitionID)
				if err != nil {
					return fmt.Errorf("read current consumer position: %w", err)
				}
				if err := activeReader.SetPosition(topicName, partitionID, nextOffset); err != nil {
					return fmt.Errorf("set consumer position: %w", err)
				}
				break
			}
		}
	}

	key := offsets.Key{GroupID: groupID, Topic: topicName, PartitionID: partitionID}
	if err := c.offsetStore.Reset(key, nextOffset, reason); err != nil {
		if activeReader != nil {
			rollbackErr := activeReader.SetPosition(topicName, partitionID, previousPosition)
			return errors.Join(
				fmt.Errorf("persist offset reset: %w", err),
				wrapRollbackError(rollbackErr),
			)
		}
		return fmt.Errorf("persist offset reset: %w", err)
	}
	return nil
}

// CommittedOffset returns the durable committed state for a group partition.
// It does not require the group to have active members.
func (c *Coordinator) CommittedOffset(
	groupID string,
	topicName string,
	partitionID uint32,
) (offsets.Commit, bool, error) {
	if c == nil || c.offsetStore == nil {
		return offsets.Commit{}, false, ErrOffsetStoreNotConfigured
	}
	return c.offsetStore.Get(offsets.Key{
		GroupID:     groupID,
		Topic:       topicName,
		PartitionID: partitionID,
	})
}

// ResetOffset intentionally changes a group's committed next offset and the
// active member's in-memory cursor. The target must be within the partition's
// current retained range. If durable reset fails, the old cursor is restored
// best-effort.
func (c *Coordinator) ResetOffset(
	groupID string,
	memberID string,
	generation uint64,
	topicName string,
	partitionID uint32,
	nextOffset uint64,
	reason string,
) error {
	if c == nil || c.offsetStore == nil {
		return ErrOffsetStoreNotConfigured
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, member, partition, err := c.commitAuthorityLocked(
		groupID,
		memberID,
		generation,
		topicName,
		partitionID,
	)
	if err != nil {
		return err
	}

	start, end, err := partition.OffsetRange()
	if err != nil {
		return fmt.Errorf("read partition offset range: %w", err)
	}
	if nextOffset < start || nextOffset > end {
		return fmt.Errorf(
			"%w: requested=%d available=[%d,%d]",
			ErrCommittedOffsetOutOfRange,
			nextOffset,
			start,
			end,
		)
	}

	previousPosition, err := member.reader.Position(topicName, partitionID)
	if err != nil {
		return fmt.Errorf("read current consumer position: %w", err)
	}
	if err := member.reader.SetPosition(topicName, partitionID, nextOffset); err != nil {
		return fmt.Errorf("set consumer position: %w", err)
	}

	key := offsets.Key{
		GroupID:     groupID,
		Topic:       topicName,
		PartitionID: partitionID,
	}
	if err := c.offsetStore.Reset(key, nextOffset, reason); err != nil {
		rollbackErr := member.reader.SetPosition(topicName, partitionID, previousPosition)
		return errors.Join(
			fmt.Errorf("persist offset reset: %w", err),
			wrapRollbackError(rollbackErr),
		)
	}

	return nil
}

func (c *Coordinator) commitAuthorityLocked(
	groupID string,
	memberID string,
	generation uint64,
	topicName string,
	partitionID uint32,
) (*groupState, *memberState, *topic.Partition, error) {
	if c == nil {
		return nil, nil, nil, ErrGroupNotFound
	}
	g, exists := c.groups[groupID]
	if !exists {
		return nil, nil, nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupID)
	}
	if generation != g.generation {
		return nil, nil, nil, fmt.Errorf(
			"%w: supplied=%d current=%d",
			ErrStaleGeneration,
			generation,
			g.generation,
		)
	}
	if g.state != StateStable {
		return nil, nil, nil, fmt.Errorf("group %q is not stable", groupID)
	}
	member, exists := g.members[memberID]
	if !exists {
		return nil, nil, nil, fmt.Errorf("%w: %q", ErrMemberNotFound, memberID)
	}
	if !containsAssignment(member.assignments, topicName, partitionID) {
		return nil, nil, nil, fmt.Errorf(
			"%w: member=%q topic=%q partition=%d",
			ErrOffsetCommitNotOwned,
			memberID,
			topicName,
			partitionID,
		)
	}
	if member.reader == nil {
		return nil, nil, nil, ErrPartitionNotOwned
	}
	partition, err := member.reader.GetPartition(topicName, partitionID)
	if err != nil {
		return nil, nil, nil, err
	}
	return g, member, partition, nil
}

func wrapRollbackError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("restore consumer position after failed reset: %w", err)
}
