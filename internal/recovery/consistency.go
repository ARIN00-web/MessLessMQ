package recovery

import (
	"fmt"
	"sort"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

// ValidateMetadataSnapshot checks the structural invariants expected of a
// committed cluster metadata snapshot.
func ValidateMetadataSnapshot(snapshot controller.MetadataSnapshot) error {
	for id, broker := range snapshot.Brokers {
		if id == "" || broker.ID != id || broker.Address == "" {
			return fmt.Errorf("broker map key %q: %w", id, ErrControllerStateInconsistent)
		}
	}
	for key, assignment := range snapshot.Partitions {
		if key.Topic == "" || key.Partition < 0 || assignment.Key != key || assignment.LeaderEpoch == 0 || len(assignment.Replicas) == 0 {
			return fmt.Errorf("partition %q/%d: %w", key.Topic, key.Partition, ErrControllerStateInconsistent)
		}
		seen := make(map[string]struct{}, len(assignment.Replicas))
		leaderFound := false
		for _, replicaID := range assignment.Replicas {
			if _, ok := snapshot.Brokers[replicaID]; !ok {
				return fmt.Errorf("partition %q/%d references unknown broker %q: %w", key.Topic, key.Partition, replicaID, ErrControllerStateInconsistent)
			}
			if _, duplicate := seen[replicaID]; duplicate {
				return fmt.Errorf("partition %q/%d repeats broker %q: %w", key.Topic, key.Partition, replicaID, ErrControllerStateInconsistent)
			}
			seen[replicaID] = struct{}{}
			leaderFound = leaderFound || replicaID == assignment.Leader
		}
		if !leaderFound {
			return fmt.Errorf("partition %q/%d leader is not a replica: %w", key.Topic, key.Partition, ErrControllerStateInconsistent)
		}
	}
	return nil
}

// ValidateControllerStatuses checks per-node Raft index invariants and that
// the cluster's advertised leader is the only node reporting the leader role.
func ValidateControllerStatuses(leaderID string, statuses map[string]controller.NodeStatus) error {
	if len(statuses) == 0 {
		return ErrControllerStateInconsistent
	}
	leaders := make([]string, 0, 1)
	for id, status := range statuses {
		if id == "" || status.ID != id || status.AppliedIndex > status.CommitIndex || status.CommitIndex > status.LastLogIndex {
			return fmt.Errorf("controller %q indexes/status: %w", id, ErrControllerStateInconsistent)
		}
		if status.Role == controller.RoleLeader {
			leaders = append(leaders, id)
		}
	}
	if len(leaders) > 1 {
		return fmt.Errorf("multiple controller leaders %v: %w", leaders, ErrControllerStateInconsistent)
	}
	if leaderID == "" {
		if len(leaders) != 0 {
			return ErrControllerStateInconsistent
		}
		return nil
	}
	status, exists := statuses[leaderID]
	if !exists || status.Role != controller.RoleLeader || len(leaders) != 1 || leaders[0] != leaderID {
		return fmt.Errorf("advertised leader %q does not match node roles: %w", leaderID, ErrControllerStateInconsistent)
	}
	return nil
}

// ValidateGroupSnapshot exposes the same ownership consistency check used by
// RecoverConsumerGroup without invoking a rebalance callback.
func ValidateGroupSnapshot(snapshot ConsumerGroupSnapshot) error {
	if snapshot.GroupID == "" || snapshot.Generation == 0 || !snapshot.Stable {
		return ErrInvalidGroupSnapshot
	}
	seenMembers := make(map[string]struct{}, len(snapshot.Members))
	owners := make(map[GroupPartition]string)
	for _, member := range snapshot.Members {
		if member.MemberID == "" {
			return ErrInvalidGroupSnapshot
		}
		if _, duplicate := seenMembers[member.MemberID]; duplicate {
			return ErrInvalidGroupSnapshot
		}
		seenMembers[member.MemberID] = struct{}{}
		for _, partition := range member.Assignments {
			if partition.Topic == "" {
				return ErrInvalidGroupSnapshot
			}
			if previous, duplicate := owners[partition]; duplicate {
				return fmt.Errorf("partition %s/%d owned by %s and %s: %w", partition.Topic, partition.PartitionID, previous, member.MemberID, ErrDuplicatePartitionOwner)
			}
			owners[partition] = member.MemberID
		}
	}
	return nil
}

// SortedControllerIDs returns stable controller IDs for deterministic
// diagnostics and recovery ordering.
func SortedControllerIDs(statuses map[string]controller.NodeStatus) []string {
	ids := make([]string, 0, len(statuses))
	for id := range statuses {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// ValidateReplicaReadiness verifies the minimum progress evidence required
// before a replica can re-enter the ISR or appear in a new assignment.
func ValidateReplicaReadiness(progress ReplicaReadiness) error {
	if progress.BrokerID == "" || progress.SourceLeader == "" || progress.LeaderEpoch == 0 ||
		!progress.Available || !progress.InSync || progress.MatchOffset < progress.HighWatermark {
		return ErrReplicaNotReady
	}
	return nil
}
