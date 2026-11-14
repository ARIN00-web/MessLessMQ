package controller

import "errors"

var (
	ErrInvalidConfig       = errors.New("controller: invalid configuration")
	ErrNotCandidate        = errors.New("controller: node is not a candidate")
	ErrNotLeader           = errors.New("controller: node is not the current leader")
	ErrNoQuorum            = errors.New("controller: quorum not reached")
	ErrNodeUnavailable     = errors.New("controller: node unavailable")
	ErrUnknownNode         = errors.New("controller: unknown node")
	ErrInvalidCommand      = errors.New("controller: invalid metadata command")
	ErrMetadataConflict    = errors.New("controller: metadata conflict")
	ErrLogInvariant        = errors.New("controller: log invariant violated")
	ErrAlreadyExists       = errors.New("controller: metadata entry already exists")
	ErrBrokerNotRegistered = errors.New("controller: broker is not registered")
	ErrLeaderNotInReplicas = errors.New("controller: partition leader must be a replica")
)

type Role uint8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}

type Broker struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type PartitionKey struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

type PartitionAssignment struct {
	Key      PartitionKey `json:"key"`
	Replicas []string     `json:"replicas"`
	// ISR is the controller-committed in-sync subset of Replicas.  It is kept
	// separate from the replica assignment so a temporarily slow or failed
	// replica does not change placement.
	ISR         []string `json:"isr,omitempty"`
	Leader      string   `json:"leader"`
	LeaderEpoch uint64   `json:"leader_epoch"`
}

type CommandType uint8

const (
	CommandRegisterBroker CommandType = iota + 1
	CommandAssignPartition
	CommandUpdatePartitionISR
	CommandNoop
)

type Command struct {
	Type       CommandType         `json:"type"`
	Broker     Broker              `json:"broker,omitempty"`
	Assignment PartitionAssignment `json:"assignment,omitempty"`
}

type LogEntry struct {
	Index   uint64  `json:"index"`
	Term    uint64  `json:"term"`
	Command Command `json:"command"`
}

type VoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term              uint64
	LeaderID          string
	PreviousLogIndex  uint64
	PreviousLogTerm   uint64
	Entries           []LogEntry
	LeaderCommitIndex uint64
}

type AppendEntriesResponse struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
}

type NodeStatus struct {
	ID           string
	Role         Role
	Term         uint64
	VotedFor     string
	LeaderID     string
	LastLogIndex uint64
	CommitIndex  uint64
	AppliedIndex uint64
}
