package replication

import (
	"errors"
	"fmt"
)

// ReplicaID identifies one physical copy of a topic partition.
type ReplicaID string

// Role describes a replica's role within one partition's replication group.
type Role uint8

const (
	RoleFollower Role = iota + 1
	RoleLeader
	RoleOffline
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleLeader:
		return "leader"
	case RoleOffline:
		return "offline"
	default:
		return "unknown"
	}
}

var (
	ErrInvalidReplicaID          = errors.New("invalid replica ID")
	ErrInvalidConfig             = errors.New("invalid replication configuration")
	ErrDuplicateReplica          = errors.New("duplicate replica ID")
	ErrPartitionMismatch         = errors.New("replicas do not represent the same topic partition")
	ErrReplicaNotFound           = errors.New("replica not found")
	ErrReplicaUnavailable        = errors.New("replica is unavailable")
	ErrNotLeader                 = errors.New("replica is not the partition leader")
	ErrNoLeader                  = errors.New("partition has no available leader")
	ErrStaleLeaderEpoch          = errors.New("stale or unexpected leader epoch")
	ErrReplicationGap            = errors.New("replication offset gap")
	ErrDivergentReplica          = errors.New("replica contains different data at the same offset")
	ErrReplicaBehindRetention    = errors.New("replica is behind the leader retention boundary")
	ErrReplicaAheadOfLeader      = errors.New("replica log end is ahead of the leader")
	ErrReplicaNotCaughtUp        = errors.New("replica is not caught up to the high watermark")
	ErrInsufficientISR           = errors.New("insufficient in-sync replicas")
	ErrNoEligibleLeader          = errors.New("no eligible in-sync replica for leader election")
	ErrInvalidReplicationData    = errors.New("invalid replication data")
	ErrInvalidReplicationBatch   = errors.New("invalid replication batch")
	ErrHighWatermarkRange        = errors.New("offset is outside the high-watermark range")
	ErrOffsetBeforeHighWatermark = errors.New("offset is before the retained log start")
	ErrRecordTooLarge            = errors.New("record exceeds committed-read byte limit")
)

const (
	ProtocolVersion                uint16 = 1
	MaxReplicationRecords                 = 4096
	MaxReplicationValueBytes              = 32 * 1024 * 1024
	MaxReplicationFrameBytes              = MaxReplicationValueBytes + 1*1024*1024
	DefaultReplicationBatchRecords        = 128
	DefaultReplicationBatchBytes          = 4 * 1024 * 1024
)

// Config controls replication for one topic partition.
type Config struct {
	LeaderID                ReplicaID
	MinISR                  int
	ReplicationBatchRecords int
	ReplicationBatchBytes   int
}

// Record is a timestamped log record transferred between replicas.
type Record struct {
	Offset    uint64
	Timestamp int64
	Value     []byte
}

// ReplicationBatch is the logical binary-protocol payload exchanged by a
// leader and follower. Network transport is intentionally added later, when
// inter-broker communication is introduced.
type ReplicationBatch struct {
	Version     uint16
	Topic       string
	PartitionID uint32
	LeaderID    ReplicaID
	LeaderEpoch uint64
	Records     []Record
}

// AppendResult describes an append and the replication progress observed by
// the leader. HighWatermark is exclusive: offsets below it are committed.
type AppendResult struct {
	Offset               uint64
	HighWatermark        uint64
	LeaderEpoch          uint64
	AcknowledgedReplicas int
}

// ReplicaStatus is an immutable snapshot of one replica.
type ReplicaStatus struct {
	ID          ReplicaID
	Role        Role
	Available   bool
	MatchOffset uint64
	LeaderEpoch uint64
}

// Status is a deterministic snapshot of a partition replication group.
type Status struct {
	Topic         string
	PartitionID   uint32
	LeaderID      ReplicaID
	LeaderEpoch   uint64
	HighWatermark uint64
	MinISR        int
	ISR           []ReplicaID
	Replicas      []ReplicaStatus
}

func validateReplicaID(id ReplicaID) error {
	if id == "" || len(id) > 128 {
		return fmt.Errorf("%w: must contain 1-128 characters", ErrInvalidReplicaID)
	}
	for _, r := range string(id) {
		if r < 0x21 || r > 0x7e {
			return fmt.Errorf("%w: ID must contain printable ASCII without spaces", ErrInvalidReplicaID)
		}
	}
	return nil
}
