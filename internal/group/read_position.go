package group

import "fmt"

// SetReadPosition changes a member's in-memory next-read cursor without
// persisting a committed offset. It is used by delivery processors to rewind
// after an application-processing failure. The caller must still hold the
// current group generation and own the partition.
func (c *Coordinator) SetReadPosition(
	groupID string,
	memberID string,
	generation uint64,
	topicName string,
	partitionID uint32,
	nextOffset uint64,
) error {
	if c == nil {
		return ErrGroupNotFound
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, member, _, err := c.commitAuthorityLocked(
		groupID,
		memberID,
		generation,
		topicName,
		partitionID,
	)
	if err != nil {
		return err
	}
	if err := member.reader.SetPosition(topicName, partitionID, nextOffset); err != nil {
		return fmt.Errorf("set in-memory read position: %w", err)
	}
	return nil
}
