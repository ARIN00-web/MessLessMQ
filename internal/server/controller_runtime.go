package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/config"
	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/interbroker"
	"github.com/ARIN00-web/messlessmq/internal/membership"
)

// controllerRuntime runs a fixed-membership Raft controller over the existing
// inter-broker TCP transport. The voter set is explicit and durable; discovered
// brokers outside that set are not silently added as voters.
type controllerRuntime struct {
	brokerID  string
	voters    []string
	node      *controller.DurableNode
	members   *membership.Registry
	transport *interbroker.Transport
	logger    *slog.Logger

	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration
	rpcTimeout  time.Duration

	requestID         atomic.Uint64
	contact           chan struct{}
	lastLeaderContact atomic.Int64
	lastQuorumContact atomic.Int64
	proposalMu        sync.Mutex
	progressMu        sync.Mutex
	nextIndex         map[string]uint64
	matchIndex        map[string]uint64
	peerReplicationMu map[string]*sync.Mutex
	onCommit          func()
}

type ControllerRuntimeStatus struct {
	BrokerID       string   `json:"broker_id"`
	Role           string   `json:"role"`
	Term           uint64   `json:"term"`
	VotedFor       string   `json:"voted_for,omitempty"`
	LeaderID       string   `json:"leader_id,omitempty"`
	LastLogIndex   uint64   `json:"last_log_index"`
	CommitIndex    uint64   `json:"commit_index"`
	AppliedIndex   uint64   `json:"applied_index"`
	Voters         []string `json:"voters"`
	KnownVoters    int      `json:"known_voters"`
	VoterCount     int      `json:"voter_count"`
	Ready          bool     `json:"ready"`
	AuthorityValid bool     `json:"authority_valid"`
}

func newControllerRuntime(brokerID string, voters []string, node *controller.DurableNode, members *membership.Registry, cfg config.Config, logger *slog.Logger) (*controllerRuntime, error) {
	if brokerID == "" || len(voters) == 0 || node == nil || members == nil {
		return nil, errors.New("controller runtime requires broker identity, voters, durable node, and membership")
	}
	voterCopy := append([]string(nil), voters...)
	sort.Strings(voterCopy)
	if logger == nil {
		logger = slog.Default()
	}
	return &controllerRuntime{
		brokerID: brokerID, voters: voterCopy, node: node, members: members,
		transport: interbroker.NewTransport(cfg.RequestTimeout), logger: logger,
		electionMin: cfg.ElectionTimeoutMin, electionMax: cfg.ElectionTimeoutMax,
		heartbeat: cfg.HeartbeatInterval, rpcTimeout: minDuration(cfg.RequestTimeout, 500*time.Millisecond),
		contact: make(chan struct{}, 1), nextIndex: make(map[string]uint64),
		matchIndex: make(map[string]uint64), peerReplicationMu: make(map[string]*sync.Mutex),
	}, nil
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 || a > b {
		return b
	}
	return a
}

func (r *controllerRuntime) NotifyLeaderContact() {
	if r == nil {
		return
	}
	r.lastLeaderContact.Store(time.Now().UnixNano())
	select {
	case r.contact <- struct{}{}:
	default:
	}
}

// HasAuthority reports whether this broker has a recent controller lease.
// A controller leader renews the lease only after a majority acknowledges its
// heartbeat; followers renew it only after accepting the current leader's
// AppendEntries. The short lease makes isolated stale brokers fail closed
// before a replacement controller can promote a new partition leader.
func (r *controllerRuntime) HasAuthority() bool {
	if r == nil || r.node == nil {
		return false
	}
	status := r.node.Status()
	lease := 2 * r.heartbeat
	if lease <= 0 {
		lease = 300 * time.Millisecond
	}
	now := time.Now()
	switch status.Role {
	case controller.RoleLeader:
		last := r.lastQuorumContact.Load()
		return last > 0 && now.Sub(time.Unix(0, last)) <= lease
	case controller.RoleFollower:
		if status.LeaderID == "" {
			return false
		}
		last := r.lastLeaderContact.Load()
		return last > 0 && now.Sub(time.Unix(0, last)) <= lease
	default:
		return false
	}
}

func (r *controllerRuntime) Status() ControllerRuntimeStatus {
	if r == nil || r.node == nil {
		return ControllerRuntimeStatus{}
	}
	status := r.node.Status()
	known := 0
	for _, voter := range r.voters {
		if _, ok := r.members.Broker(voter); ok {
			known++
		}
	}
	return ControllerRuntimeStatus{
		BrokerID: status.ID, Role: status.Role.String(), Term: status.Term,
		VotedFor: status.VotedFor, LeaderID: status.LeaderID,
		LastLogIndex: status.LastLogIndex, CommitIndex: status.CommitIndex,
		AppliedIndex: status.AppliedIndex, Voters: append([]string(nil), r.voters...),
		KnownVoters: known, VoterCount: len(r.voters), Ready: known == len(r.voters), AuthorityValid: r.HasAuthority(),
	}
}

func (r *controllerRuntime) Start(ctx context.Context) {
	if r == nil || ctx == nil {
		return
	}
	go r.run(ctx)
}

func (r *controllerRuntime) run(ctx context.Context) {
	seed := time.Now().UnixNano()
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(r.brokerID))
	rng := rand.New(rand.NewSource(seed + int64(hash.Sum64())))
	electionDeadline := time.Now().Add(r.nextElectionDelay(rng))
	nextHeartbeat := time.Time{}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.contact:
			electionDeadline = time.Now().Add(r.nextElectionDelay(rng))
		case now := <-ticker.C:
			if !r.votersReady() {
				continue
			}
			status := r.node.Status()
			if status.Role == controller.RoleLeader {
				if nextHeartbeat.IsZero() || !now.Before(nextHeartbeat) {
					r.sendHeartbeats(ctx)
					nextHeartbeat = time.Now().Add(r.heartbeat)
				}
				continue
			}
			nextHeartbeat = time.Time{}
			if !now.Before(electionDeadline) {
				if err := r.campaign(ctx); err != nil && !errors.Is(err, controller.ErrNoQuorum) && !errors.Is(err, context.Canceled) {
					r.logger.Warn("controller election attempt failed", "broker_id", r.brokerID, "error", err)
				}
				electionDeadline = time.Now().Add(r.nextElectionDelay(rng))
			}
		}
	}
}

func (r *controllerRuntime) nextElectionDelay(rng *rand.Rand) time.Duration {
	if r.electionMax <= r.electionMin {
		return r.electionMin
	}
	span := r.electionMax - r.electionMin
	return r.electionMin + time.Duration(rng.Int63n(int64(span)))
}

func (r *controllerRuntime) votersReady() bool {
	for _, voter := range r.voters {
		if _, ok := r.members.Broker(voter); !ok {
			return false
		}
	}
	return true
}

func (r *controllerRuntime) majority() int { return len(r.voters)/2 + 1 }

func (r *controllerRuntime) campaign(parent context.Context) error {
	request, err := r.node.BeginElection()
	if err != nil {
		return err
	}
	votes := 1 // durable self-vote
	type result struct {
		peerID   string
		response controller.VoteResponse
		err      error
	}
	peers := r.peerIDs()
	results := make(chan result, len(peers))
	for _, peerID := range peers {
		peerID := peerID
		go func() {
			var response controller.VoteResponse
			err := r.callPeer(parent, peerID, interbroker.MessageVoteRequest, request, &response)
			results <- result{peerID: peerID, response: response, err: err}
		}()
	}
	for range peers {
		select {
		case <-parent.Done():
			return parent.Err()
		case result := <-results:
			if result.err != nil {
				continue
			}
			if err := r.node.RecordVote(result.peerID, result.response); err != nil {
				return err
			}
			if result.response.Term > request.Term {
				return controller.ErrNotCandidate
			}
			if result.response.Term == request.Term && result.response.VoteGranted {
				votes++
			}
		}
	}
	if votes < r.majority() {
		return controller.ErrNoQuorum
	}
	if err := r.node.WinElection(); err != nil {
		return err
	}
	r.logger.Info("controller elected", "broker_id", r.brokerID, "term", request.Term, "votes", votes, "voters", len(r.voters))
	// A current-term no-op is the Raft commit barrier for any valid older-term
	// suffix inherited from the previous leader. It prevents the metadata
	// reconciler from appending duplicate registrations over pending entries.
	if err := r.Propose(parent, controller.Command{Type: controller.CommandNoop}); err != nil {
		r.logger.Warn("controller elected but could not commit initial barrier", "broker_id", r.brokerID, "error", err)
	}
	r.sendHeartbeats(parent)
	return nil
}

// Propose commits one metadata command through the live fixed-voter Raft
// quorum. The leader persists its entry first, replicates it to a majority,
// commits locally, then advertises the new commit index to followers.
func (r *controllerRuntime) Propose(parent context.Context, command controller.Command) error {
	return r.propose(parent, command, nil)
}

// ProposeAtVersion performs an optimistic metadata compare-and-swap. It is
// used by multi-step admin workflows so a concurrent ISR/leader update cannot
// be overwritten by a proposal built from an older metadata snapshot.
func (r *controllerRuntime) ProposeAtVersion(parent context.Context, command controller.Command, expectedVersion uint64) error {
	return r.propose(parent, command, &expectedVersion)
}

func (r *controllerRuntime) propose(parent context.Context, command controller.Command, expectedVersion *uint64) error {
	if r == nil || r.node == nil || parent == nil {
		return controller.ErrInvalidConfig
	}
	r.proposalMu.Lock()
	defer r.proposalMu.Unlock()
	if expectedVersion != nil && r.node.Metadata().Version != *expectedVersion {
		return controller.ErrMetadataConflict
	}

	initial := r.node.Status()
	if initial.Role != controller.RoleLeader || initial.LeaderID != r.brokerID {
		return controller.ErrNotLeader
	}
	entry, err := r.node.AppendLocal(command)
	if err != nil {
		return err
	}
	type result struct {
		peerID string
		err    error
	}
	peers := r.peerIDs()
	results := make(chan result, len(peers))
	for _, peerID := range peers {
		peerID := peerID
		go func() {
			_, replicateErr := r.replicatePeer(parent, peerID, entry.Term)
			results <- result{peerID: peerID, err: replicateErr}
		}()
	}

	replicated := 1 // the leader's own durable log entry
	for range peers {
		select {
		case <-parent.Done():
			return parent.Err()
		case result := <-results:
			if result.err == nil {
				replicated++
			} else if errors.Is(result.err, controller.ErrNotLeader) {
				return result.err
			}
		}
	}

	if replicated < r.majority() {
		return controller.ErrNoQuorum
	}
	current := r.node.Status()
	if current.Role != controller.RoleLeader || current.Term != entry.Term {
		return controller.ErrNotLeader
	}
	if err := r.node.CommitCurrentTermEntry(replicated); err != nil {
		return err
	}
	if r.onCommit != nil {
		r.onCommit()
	}

	// Followers learn the committed index from the next AppendEntries. Send it
	// immediately rather than waiting for the periodic heartbeat; the regular
	// heartbeat remains a retry path for temporarily unavailable followers.
	r.sendHeartbeats(parent)
	return nil
}

func (r *controllerRuntime) sendHeartbeats(parent context.Context) {
	status := r.node.Status()
	if status.Role != controller.RoleLeader {
		return
	}
	type result struct {
		peerID string
		err    error
	}
	peers := r.peerIDs()
	results := make(chan result, len(peers))
	for _, peerID := range peers {
		peerID := peerID
		go func() {
			_, err := r.replicatePeer(parent, peerID, status.Term)
			results <- result{peerID: peerID, err: err}
		}()
	}
	acknowledgements := 1 // this node's durable state
	if acknowledgements >= r.majority() {
		r.lastQuorumContact.Store(time.Now().UnixNano())
		return
	}
	for received := 0; received < len(peers); received++ {
		select {
		case <-parent.Done():
			return
		case result := <-results:
			if result.err == nil {
				acknowledgements++
			}
			if acknowledgements >= r.majority() {
				current := r.node.Status()
				if current.Role == controller.RoleLeader && current.Term == status.Term {
					r.lastQuorumContact.Store(time.Now().UnixNano())
				}
				return
			}
		}
	}
}

// replicatePeer maintains Raft's per-follower nextIndex/matchIndex state and
// backs nextIndex up using the follower's conflict hint until the log prefix
// matches. This avoids retransmitting the complete controller log on every
// heartbeat/proposal. It deliberately has no snapshot fallback: nextIndex is
// bounded by the retained log's first index (1) until snapshot installation is
// implemented as a coordinated storage/recovery feature.
func (r *controllerRuntime) replicatePeer(parent context.Context, peerID string, term uint64) (uint64, error) {
	if parent == nil {
		return 0, controller.ErrInvalidConfig
	}
	lock := r.peerReplicationLock(peerID)
	lock.Lock()
	defer lock.Unlock()

	status := r.node.Status()
	if status.Role != controller.RoleLeader || status.Term != term || status.LeaderID != r.brokerID {
		return 0, controller.ErrNotLeader
	}
	nextIndex := r.followerNextIndex(peerID, status.LastLogIndex+1)
	maxAttempts := int(status.LastLogIndex) + 2
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := parent.Err(); err != nil {
			return 0, err
		}
		request := r.node.AppendRequestFrom(nextIndex)
		if request.Term != term || request.LeaderID != r.brokerID {
			return 0, controller.ErrNotLeader
		}
		var response controller.AppendEntriesResponse
		if err := r.callPeer(parent, peerID, interbroker.MessageAppendEntries, request, &response); err != nil {
			return 0, err
		}
		if response.Term > term {
			if err := r.node.RecordVote(peerID, controller.VoteResponse{Term: response.Term}); err != nil {
				return 0, err
			}
			return 0, controller.ErrNotLeader
		}
		if response.Term != term {
			return 0, controller.ErrNotLeader
		}
		if response.Success {
			matched := request.PreviousLogIndex + uint64(len(request.Entries))
			r.setFollowerProgress(peerID, matched+1, matched)
			return matched, nil
		}

		candidate := response.ConflictIndex
		if candidate == 0 || candidate >= nextIndex {
			if nextIndex <= 1 {
				return 0, fmt.Errorf("follower %s rejected log prefix at index 1: %w", peerID, controller.ErrLogInvariant)
			}
			candidate = nextIndex - 1
		}
		if candidate < 1 {
			candidate = 1
		}
		r.setFollowerNextIndex(peerID, candidate)
		nextIndex = candidate
	}
	return 0, fmt.Errorf("follower %s did not converge after %d AppendEntries attempts: %w", peerID, maxAttempts, controller.ErrLogInvariant)
}

func (r *controllerRuntime) peerReplicationLock(peerID string) *sync.Mutex {
	r.progressMu.Lock()
	defer r.progressMu.Unlock()
	lock := r.peerReplicationMu[peerID]
	if lock == nil {
		lock = &sync.Mutex{}
		r.peerReplicationMu[peerID] = lock
	}
	return lock
}

func (r *controllerRuntime) followerNextIndex(peerID string, fallback uint64) uint64 {
	r.progressMu.Lock()
	defer r.progressMu.Unlock()
	if next, ok := r.nextIndex[peerID]; ok && next > 0 {
		return next
	}
	if fallback < 1 {
		fallback = 1
	}
	r.nextIndex[peerID] = fallback
	return fallback
}

func (r *controllerRuntime) setFollowerNextIndex(peerID string, next uint64) {
	if next < 1 {
		next = 1
	}
	r.progressMu.Lock()
	r.nextIndex[peerID] = next
	r.progressMu.Unlock()
}

func (r *controllerRuntime) setFollowerProgress(peerID string, next, matched uint64) {
	if next < 1 {
		next = 1
	}
	r.progressMu.Lock()
	r.nextIndex[peerID] = next
	r.matchIndex[peerID] = matched
	r.progressMu.Unlock()
}

func (r *controllerRuntime) peerIDs() []string {
	peers := make([]string, 0, len(r.voters)-1)
	for _, voter := range r.voters {
		if voter != r.brokerID {
			peers = append(peers, voter)
		}
	}
	return peers
}

func (r *controllerRuntime) callPeer(parent context.Context, peerID string, messageType interbroker.MessageType, input, output any) error {
	peer, ok := r.members.Broker(peerID)
	if !ok {
		return fmt.Errorf("controller voter %s is not in membership: %w", peerID, membership.ErrUnknownPeer)
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, r.rpcTimeout)
	defer cancel()
	response, err := r.transport.RoundTrip(ctx, peer.PeerAddress, interbroker.Envelope{
		Type: messageType, RequestID: r.requestID.Add(1), SourceID: r.brokerID,
		TargetID: peerID, Payload: payload,
	})
	if err != nil {
		return err
	}
	if err := json.Unmarshal(response.Payload, output); err != nil {
		return fmt.Errorf("decode controller response from %s: %w", peerID, err)
	}
	return nil
}
