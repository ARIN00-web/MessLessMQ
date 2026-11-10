package recovery

import "fmt"

type GroupPartition struct {
	Topic       string
	PartitionID uint32
}

type GroupMemberSnapshot struct {
	MemberID    string
	Assignments []GroupPartition
}

type ConsumerGroupSnapshot struct {
	GroupID    string
	Generation uint64
	Stable     bool
	Members    []GroupMemberSnapshot
}

type ConsumerGroupRecoveryFunc func(groupID string) (ConsumerGroupSnapshot, error)

type ConsumerGroupRecoveryResult struct {
	GroupID    string
	Generation uint64
	Members    int
	Partitions int
}

// RecoverConsumerGroup asks the group's coordinator to rebalance and then
// validates that the returned stable assignment has exactly one owner for
// every partition. The callback normally wraps Coordinator.Rebalance and
// Coordinator.DescribeGroup. Durable offsets are deliberately not rewritten.
func RecoverConsumerGroup(groupID string, recoverFn ConsumerGroupRecoveryFunc) (ConsumerGroupRecoveryResult, error) {
	if groupID == "" || recoverFn == nil {
		return ConsumerGroupRecoveryResult{}, ErrInvalidConfig
	}
	snapshot, err := recoverFn(groupID)
	if err != nil {
		return ConsumerGroupRecoveryResult{}, fmt.Errorf("recover group %q: %w", groupID, err)
	}
	if snapshot.GroupID != groupID || snapshot.Generation == 0 || !snapshot.Stable {
		return ConsumerGroupRecoveryResult{}, ErrInvalidGroupSnapshot
	}
	owners := make(map[GroupPartition]string)
	members := make(map[string]struct{}, len(snapshot.Members))
	for _, member := range snapshot.Members {
		if member.MemberID == "" {
			return ConsumerGroupRecoveryResult{}, ErrInvalidGroupSnapshot
		}
		if _, duplicate := members[member.MemberID]; duplicate {
			return ConsumerGroupRecoveryResult{}, ErrInvalidGroupSnapshot
		}
		members[member.MemberID] = struct{}{}
		for _, partition := range member.Assignments {
			if partition.Topic == "" {
				return ConsumerGroupRecoveryResult{}, ErrInvalidGroupSnapshot
			}
			if previous, duplicate := owners[partition]; duplicate {
				return ConsumerGroupRecoveryResult{}, fmt.Errorf("%s/%d owned by %q and %q: %w", partition.Topic, partition.PartitionID, previous, member.MemberID, ErrDuplicatePartitionOwner)
			}
			owners[partition] = member.MemberID
		}
	}
	return ConsumerGroupRecoveryResult{
		GroupID: groupID, Generation: snapshot.Generation,
		Members: len(members), Partitions: len(owners),
	}, nil
}
