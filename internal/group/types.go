package group

import "time"

// State describes the lifecycle state of a consumer group.
type State uint8

const (
	// StateEmpty means the group has no active members.
	StateEmpty State = iota
	// StateRebalancing means partition ownership is being recalculated.
	StateRebalancing
	// StateStable means membership and partition assignments are settled.
	StateStable
)

func (s State) String() string {
	switch s {
	case StateEmpty:
		return "Empty"
	case StateRebalancing:
		return "Rebalancing"
	case StateStable:
		return "Stable"
	default:
		return "Unknown"
	}
}

// TopicPartition identifies one partition within one topic.
type TopicPartition struct {
	Topic       string
	PartitionID uint32
}

// JoinResult is returned after a member joins and the group rebalances.
type JoinResult struct {
	GroupID     string
	MemberID    string
	Generation  uint64
	Assignments []TopicPartition
}

// MemberInfo is a read-only snapshot of one group member.
type MemberInfo struct {
	MemberID      string
	Subscriptions []string
	Assignments   []TopicPartition
	LastHeartbeat time.Time
}

// Description is a read-only snapshot of a consumer group.
type Description struct {
	GroupID    string
	Generation uint64
	State      State
	Members    []MemberInfo
}

// ExpiredMember identifies a member removed by a session-timeout sweep.
type ExpiredMember struct {
	GroupID  string
	MemberID string
}
