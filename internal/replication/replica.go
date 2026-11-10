package replication

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/ARIN00-web/messlessmq/internal/topic"
)

// Replica wraps one persistent local partition log. Its role, availability,
// leader epoch, and match offset are coordinated by Manager.
type Replica struct {
	mu        sync.RWMutex
	id        ReplicaID
	partition *topic.Partition
	role      Role
	available bool
	epoch     uint64
	match     uint64 // exclusive next offset known to exist on this replica
}

// NewReplica creates a replica around an already-open partition log.
func NewReplica(id ReplicaID, partition *topic.Partition) (*Replica, error) {
	if err := validateReplicaID(id); err != nil {
		return nil, err
	}
	if partition == nil {
		return nil, ErrPartitionMismatch
	}
	next, err := partition.NextOffset()
	if err != nil {
		return nil, fmt.Errorf("inspect replica log end: %w", err)
	}
	return &Replica{
		id:        id,
		partition: partition,
		role:      RoleFollower,
		available: true,
		match:     next,
	}, nil
}

func (r *Replica) ID() ReplicaID {
	if r == nil {
		return ""
	}
	return r.id
}

func (r *Replica) TopicName() string {
	if r == nil || r.partition == nil {
		return ""
	}
	return r.partition.TopicName()
}

func (r *Replica) PartitionID() uint32 {
	if r == nil || r.partition == nil {
		return 0
	}
	return r.partition.ID()
}

// Status returns a thread-safe immutable snapshot.
func (r *Replica) Status() ReplicaStatus {
	if r == nil {
		return ReplicaStatus{Role: RoleOffline}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return ReplicaStatus{
		ID:          r.id,
		Role:        r.role,
		Available:   r.available,
		MatchOffset: r.match,
		LeaderEpoch: r.epoch,
	}
}

func (r *Replica) setState(role Role, available bool, epoch, match uint64) {
	r.mu.Lock()
	r.role = role
	r.available = available
	r.epoch = epoch
	r.match = match
	r.mu.Unlock()
}

func (r *Replica) setAvailability(available bool) {
	r.mu.Lock()
	r.available = available
	if !available {
		r.role = RoleOffline
	} else if r.role == RoleOffline {
		r.role = RoleFollower
	}
	r.mu.Unlock()
}

// applyRecord idempotently applies one leader record. A matching record that
// already exists is accepted; gaps and conflicting payloads are rejected.
func (r *Replica) applyRecord(leaderID ReplicaID, epoch uint64, record Record) error {
	if r == nil {
		return ErrReplicaNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.available {
		return ErrReplicaUnavailable
	}
	if epoch != r.epoch && r.epoch != 0 && epoch < r.epoch {
		return fmt.Errorf("replica %q: supplied=%d current=%d: %w", r.id, epoch, r.epoch, ErrStaleLeaderEpoch)
	}
	if leaderID == r.id {
		return ErrNotLeader
	}

	next, err := r.partition.NextOffset()
	if err != nil {
		return err
	}

	matchOffset := next
	switch {
	case record.Offset > next:
		return fmt.Errorf("replica %q expected offset %d, received %d: %w", r.id, next, record.Offset, ErrReplicationGap)

	case record.Offset < next:
		value, timestamp, readErr := r.partition.ReadWithTimestamp(record.Offset)
		if readErr != nil {
			return fmt.Errorf("inspect existing offset %d: %w", record.Offset, readErr)
		}
		if !bytes.Equal(value, record.Value) || timestamp != record.Timestamp {
			return fmt.Errorf("replica %q offset %d: %w", r.id, record.Offset, ErrDivergentReplica)
		}
		// Exact duplicate: the follower already persisted this record. Keep
		// MatchOffset at the actual log end; replaying an older record must not
		// make the replica appear to move backwards.

	default:
		appended, appendErr := r.partition.AppendAtSync(record.Value, record.Timestamp)
		if appendErr != nil {
			return fmt.Errorf("append replicated offset %d: %w", record.Offset, appendErr)
		}
		if appended != record.Offset {
			return fmt.Errorf("appended offset %d, expected %d: %w", appended, record.Offset, ErrDivergentReplica)
		}
		matchOffset = appended + 1
	}

	r.epoch = epoch
	r.match = matchOffset
	return nil
}

// ApplyReplication is a low-level follower entry point used by the future
// inter-broker transport. Manager remains authoritative for ISR membership.
func (r *Replica) ApplyReplication(leaderID ReplicaID, epoch uint64, record Record) error {
	return r.applyRecord(leaderID, epoch, record)
}
