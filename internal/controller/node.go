package controller

import (
	"fmt"
	"sync"
)

// Node is one metadata-controller Raft participant. Its protocol methods are
// deterministic and transport-neutral; Cluster is the in-memory network used
// by tests and by this phase's examples.
type Node struct {
	mu sync.Mutex

	id    string
	peers []string

	currentTerm uint64
	votedFor    string
	role        Role
	leaderID    string
	votes       map[string]struct{}

	log         []LogEntry
	commitIndex uint64
	lastApplied uint64
	metadata    metadataState
}

func newNode(id string, peers []string) (*Node, error) {
	if id == "" {
		return nil, ErrInvalidConfig
	}
	seen := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer == "" || peer == id {
			return nil, ErrInvalidConfig
		}
		if _, ok := seen[peer]; ok {
			return nil, ErrInvalidConfig
		}
		seen[peer] = struct{}{}
	}
	return &Node{
		id:       id,
		peers:    append([]string(nil), peers...),
		role:     RoleFollower,
		votes:    make(map[string]struct{}),
		metadata: newMetadataState(),
	}, nil
}

func (n *Node) ID() string { return n.id }

func (n *Node) Status() NodeStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	return NodeStatus{
		ID: n.id, Role: n.role, Term: n.currentTerm, VotedFor: n.votedFor,
		LeaderID: n.leaderID, LastLogIndex: uint64(len(n.log)),
		CommitIndex: n.commitIndex, AppliedIndex: n.lastApplied,
	}
}

func (n *Node) Metadata() MetadataSnapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.metadata.snapshot()
}

func (n *Node) Log() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return cloneEntries(n.log)
}

func cloneEntries(entries []LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	copy(out, entries)
	for i := range out {
		out[i].Command.Assignment.Replicas = append([]string(nil), entries[i].Command.Assignment.Replicas...)
	}
	return out
}

func (n *Node) isPeerLocked(id string) bool {
	for _, peer := range n.peers {
		if peer == id {
			return true
		}
	}
	return false
}

func (n *Node) majority() int { return (len(n.peers)+1)/2 + 1 }

func (n *Node) lastLogLocked() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) termAtLocked(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index > uint64(len(n.log)) {
		return 0, false
	}
	return n.log[index-1].Term, true
}

// BeginElection starts a new term and votes for this node itself.
func (n *Node) BeginElection() (VoteRequest, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.currentTerm++
	n.role = RoleCandidate
	n.leaderID = ""
	n.votedFor = n.id
	n.votes = map[string]struct{}{n.id: {}}
	lastIndex, lastTerm := n.lastLogLocked()
	return VoteRequest{Term: n.currentTerm, CandidateID: n.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}, nil
}

// HandleVoteRequest implements Raft's term and log-freshness checks.
func (n *Node) HandleVoteRequest(request VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	response := VoteResponse{Term: n.currentTerm}
	if request.CandidateID == "" || !n.isPeerLocked(request.CandidateID) || request.Term < n.currentTerm {
		return response
	}
	if request.Term > n.currentTerm {
		n.currentTerm = request.Term
		n.votedFor = ""
		n.role = RoleFollower
		n.leaderID = ""
		n.votes = make(map[string]struct{})
	}
	lastIndex, lastTerm := n.lastLogLocked()
	upToDate := request.LastLogTerm > lastTerm || (request.LastLogTerm == lastTerm && request.LastLogIndex >= lastIndex)
	if upToDate && (n.votedFor == "" || n.votedFor == request.CandidateID) {
		n.votedFor = request.CandidateID
		response.VoteGranted = true
	}
	response.Term = n.currentTerm
	return response
}

func (n *Node) recordVote(peerID string, response VoteResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if response.Term > n.currentTerm {
		n.currentTerm = response.Term
		n.votedFor = ""
		n.role = RoleFollower
		n.leaderID = ""
		n.votes = make(map[string]struct{})
		return nil
	}
	if n.role != RoleCandidate || response.Term != n.currentTerm || !response.VoteGranted {
		return nil
	}
	if !n.isPeerLocked(peerID) {
		return ErrInvalidConfig
	}
	n.votes[peerID] = struct{}{}
	return nil
}

func (n *Node) winElection() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleCandidate {
		return ErrNotCandidate
	}
	if len(n.votes) < n.majority() {
		return ErrNoQuorum
	}
	n.role = RoleLeader
	n.leaderID = n.id
	return nil
}

// HandleAppendEntries implements the follower side of Raft log replication.
func (n *Node) HandleAppendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	response := AppendEntriesResponse{Term: n.currentTerm, ConflictIndex: uint64(len(n.log)) + 1}
	if request.LeaderID == "" || !n.isPeerLocked(request.LeaderID) || request.Term < n.currentTerm {
		return response
	}
	if request.Term > n.currentTerm {
		n.currentTerm = request.Term
		n.votedFor = ""
	}
	n.role = RoleFollower
	n.leaderID = request.LeaderID
	response.Term = n.currentTerm

	previousTerm, ok := n.termAtLocked(request.PreviousLogIndex)
	if !ok {
		response.ConflictIndex = uint64(len(n.log)) + 1
		return response
	}
	if previousTerm != request.PreviousLogTerm {
		conflictTerm := previousTerm
		first := request.PreviousLogIndex
		for first > 1 {
			term, _ := n.termAtLocked(first - 1)
			if term != conflictTerm {
				break
			}
			first--
		}
		response.ConflictIndex = first
		return response
	}

	for i, incoming := range request.Entries {
		expectedIndex := request.PreviousLogIndex + uint64(i) + 1
		if incoming.Index != expectedIndex || incoming.Term == 0 || incoming.Term > request.Term {
			return response
		}
		if expectedIndex <= uint64(len(n.log)) {
			existing := n.log[expectedIndex-1]
			if existing.Term == incoming.Term {
				if !commandsEqual(existing.Command, incoming.Command) {
					return response
				}
				continue
			}
			if expectedIndex <= n.commitIndex {
				return response
			}
			n.log = n.log[:expectedIndex-1]
		}
		n.log = append(n.log, cloneEntries([]LogEntry{incoming})[0])
	}

	// A follower may have an uncommitted suffix beyond the leader's log.
	// Once the leader's prefix matches, discard that stale suffix. A committed
	// suffix is never truncated because it would violate Raft safety.
	matchedEnd := request.PreviousLogIndex + uint64(len(request.Entries))
	if matchedEnd < uint64(len(n.log)) {
		if matchedEnd < n.commitIndex {
			return response
		}
		n.log = n.log[:matchedEnd]
	}

	if request.LeaderCommitIndex > n.commitIndex {
		newCommit := request.LeaderCommitIndex
		if newCommit > uint64(len(n.log)) {
			newCommit = uint64(len(n.log))
		}
		n.commitIndex = newCommit
		if err := n.applyCommittedLocked(); err != nil {
			// A committed log entry failing deterministic state-machine application
			// indicates divergent/corrupt metadata, not a recoverable RPC rejection.
			return response
		}
	}
	response.Success = true
	response.ConflictIndex = uint64(len(n.log)) + 1
	return response
}

func commandsEqual(a, b Command) bool {
	if a.Type != b.Type || a.Broker != b.Broker || a.Assignment.Key != b.Assignment.Key || a.Assignment.Leader != b.Assignment.Leader || a.Assignment.LeaderEpoch != b.Assignment.LeaderEpoch || len(a.Assignment.Replicas) != len(b.Assignment.Replicas) {
		return false
	}
	for i := range a.Assignment.Replicas {
		if a.Assignment.Replicas[i] != b.Assignment.Replicas[i] {
			return false
		}
	}
	return true
}

func (n *Node) appendLocal(command Command) (LogEntry, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader || n.leaderID != n.id {
		return LogEntry{}, ErrNotLeader
	}
	projected := n.metadata.clone()
	for index := n.lastApplied; index < uint64(len(n.log)); index++ {
		if err := projected.apply(n.log[index].Command); err != nil {
			return LogEntry{}, fmt.Errorf("%w: invalid uncommitted suffix", ErrLogInvariant)
		}
	}
	if err := projected.validate(command); err != nil {
		return LogEntry{}, err
	}
	entry := LogEntry{Index: uint64(len(n.log)) + 1, Term: n.currentTerm, Command: command}
	entry.Command.Assignment.Replicas = append([]string(nil), command.Assignment.Replicas...)
	n.log = append(n.log, entry)
	return entry, nil
}

func (n *Node) appendRequest() AppendEntriesRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	return AppendEntriesRequest{
		Term: n.currentTerm, LeaderID: n.id, PreviousLogIndex: 0,
		PreviousLogTerm: 0, Entries: cloneEntries(n.log), LeaderCommitIndex: n.commitIndex,
	}
}

func (n *Node) commitCurrentTermEntry(replicated int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader || n.leaderID != n.id {
		return ErrNotLeader
	}
	if replicated < n.majority() {
		return ErrNoQuorum
	}
	lastIndex := uint64(len(n.log))
	if lastIndex == 0 || n.log[lastIndex-1].Term != n.currentTerm {
		return ErrNoQuorum
	}
	n.commitIndex = lastIndex
	return n.applyCommittedLocked()
}

func (n *Node) applyCommittedLocked() error {
	for n.lastApplied < n.commitIndex {
		index := n.lastApplied
		if index >= uint64(len(n.log)) {
			return fmt.Errorf("%w: commit index exceeds log", ErrLogInvariant)
		}
		if err := n.metadata.apply(n.log[index].Command); err != nil {
			return fmt.Errorf("%w at index %d: %v", ErrLogInvariant, index+1, err)
		}
		n.lastApplied++
	}
	return nil
}
