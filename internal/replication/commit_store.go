package replication

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const commitStateVersion uint16 = 1

var (
	ErrInvalidCommitState  = errors.New("invalid replication commit state")
	ErrCommitRegression    = errors.New("high watermark cannot move backwards")
	ErrCommitAheadOfLog    = errors.New("high watermark cannot exceed the local log end")
	ErrCommitLeader        = errors.New("commit state belongs to a different leader or epoch")
	ErrFollowerCommitAhead = errors.New("follower high watermark exceeds the leader high watermark")
)

// CommitState records the exclusive committed offset for one physical
// partition replica. Records with offsets smaller than HighWatermark are
// committed; records at or beyond it must not be returned to consumers.
type CommitState struct {
	Version       uint16 `json:"version"`
	Topic         string `json:"topic"`
	PartitionID   uint32 `json:"partition_id"`
	HighWatermark uint64 `json:"high_watermark"`
	LeaderID      string `json:"leader_id"`
	LeaderEpoch   uint64 `json:"leader_epoch"`
}

type commitKey struct {
	topic       string
	partitionID uint32
}

// CommitStore persists per-partition high watermarks using atomic file
// replacement. It intentionally stores no open file descriptors, so each
// update is fully written, synced, renamed, and directory-synced before it is
// reported successful.
type CommitStore struct {
	mu     sync.RWMutex
	dir    string
	states map[commitKey]CommitState
}

func OpenCommitStore(dir string) (*CommitStore, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, ErrInvalidCommitState
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create commit-state directory: %w", err)
	}
	store := &CommitStore{dir: dir, states: make(map[commitKey]CommitState)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read commit-state directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".commit.json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read commit state %q: %w", entry.Name(), err)
		}
		var state CommitState
		decoderErr := decodeCommitState(data, &state)
		if decoderErr != nil {
			return nil, fmt.Errorf("decode commit state %q: %w", entry.Name(), decoderErr)
		}
		if err := validateCommitState(state); err != nil {
			return nil, fmt.Errorf("validate commit state %q: %w", entry.Name(), err)
		}
		if entry.Name() != commitFilename(state.Topic, state.PartitionID) {
			return nil, fmt.Errorf("commit state filename does not match its identity: %w", ErrInvalidCommitState)
		}
		key := commitKey{topic: state.Topic, partitionID: state.PartitionID}
		if _, exists := store.states[key]; exists {
			return nil, fmt.Errorf("duplicate commit state for %s:%d: %w", state.Topic, state.PartitionID, ErrInvalidCommitState)
		}
		store.states[key] = state
	}
	return store, nil
}

func decodeCommitState(data []byte, state *CommitState) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(state); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return ErrInvalidCommitState
		}
		return err
	}
	return nil
}

func validateCommitState(state CommitState) error {
	if state.Version != commitStateVersion || strings.TrimSpace(state.Topic) == "" ||
		strings.TrimSpace(state.LeaderID) == "" || state.LeaderEpoch == 0 {
		return ErrInvalidCommitState
	}
	return nil
}

func commitFilename(topic string, partitionID uint32) string {
	digest := sha256.Sum256([]byte(topic))
	return hex.EncodeToString(digest[:12]) + "-" + fmt.Sprintf("%010d.commit.json", partitionID)
}

// Initialize creates a partition's commit state if it does not already exist.
// initialHighWatermark is chosen by the broker runtime: standalone brokers may
// adopt their existing log end, while newly configured distributed replicas
// start at the retained log start until a leader commits data to them.
func (s *CommitStore) Initialize(topic string, partitionID uint32, initialHighWatermark, logEnd uint64, leaderID string, leaderEpoch uint64) (CommitState, error) {
	if s == nil || strings.TrimSpace(topic) == "" || strings.TrimSpace(leaderID) == "" || leaderEpoch == 0 {
		return CommitState{}, ErrInvalidCommitState
	}
	if initialHighWatermark > logEnd {
		return CommitState{}, ErrCommitAheadOfLog
	}
	key := commitKey{topic: topic, partitionID: partitionID}
	s.mu.Lock()
	defer s.mu.Unlock()
	if state, exists := s.states[key]; exists {
		if state.LeaderID != leaderID || state.LeaderEpoch != leaderEpoch {
			return CommitState{}, fmt.Errorf("stored leader=%s epoch=%d configured leader=%s epoch=%d: %w", state.LeaderID, state.LeaderEpoch, leaderID, leaderEpoch, ErrCommitLeader)
		}
		if state.HighWatermark > logEnd {
			return CommitState{}, fmt.Errorf("stored high watermark=%d log end=%d: %w", state.HighWatermark, logEnd, ErrCommitAheadOfLog)
		}
		return state, nil
	}
	state := CommitState{Version: commitStateVersion, Topic: topic, PartitionID: partitionID, HighWatermark: initialHighWatermark, LeaderID: leaderID, LeaderEpoch: leaderEpoch}
	if err := s.persist(state); err != nil {
		return CommitState{}, err
	}
	s.states[key] = state
	return state, nil
}

// Get returns the current committed boundary for a partition.
func (s *CommitStore) Get(topic string, partitionID uint32) (CommitState, bool) {
	if s == nil {
		return CommitState{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, exists := s.states[commitKey{topic: topic, partitionID: partitionID}]
	return state, exists
}

// Advance durably moves the exclusive high watermark forward. It rejects
// stale leaders, regressions, and values beyond the local durable log end.
func (s *CommitStore) Advance(topic string, partitionID uint32, highWatermark uint64, leaderID string, leaderEpoch, logEnd uint64) (CommitState, error) {
	if s == nil || strings.TrimSpace(topic) == "" || strings.TrimSpace(leaderID) == "" || leaderEpoch == 0 {
		return CommitState{}, ErrInvalidCommitState
	}
	if highWatermark > logEnd {
		return CommitState{}, ErrCommitAheadOfLog
	}
	key := commitKey{topic: topic, partitionID: partitionID}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.states[key]
	if !exists {
		return CommitState{}, fmt.Errorf("partition %s:%d has no initialized commit state: %w", topic, partitionID, ErrInvalidCommitState)
	}
	if current.LeaderID != leaderID || current.LeaderEpoch != leaderEpoch {
		return CommitState{}, fmt.Errorf("stored leader=%s epoch=%d request leader=%s epoch=%d: %w", current.LeaderID, current.LeaderEpoch, leaderID, leaderEpoch, ErrCommitLeader)
	}
	if highWatermark < current.HighWatermark {
		return CommitState{}, fmt.Errorf("current=%d requested=%d: %w", current.HighWatermark, highWatermark, ErrCommitRegression)
	}
	if highWatermark == current.HighWatermark {
		return current, nil
	}
	updated := current
	updated.HighWatermark = highWatermark
	if err := s.persist(updated); err != nil {
		return CommitState{}, err
	}
	s.states[key] = updated
	return updated, nil
}

func (s *CommitStore) persist(state CommitState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode commit state: %w", err)
	}
	path := filepath.Join(s.dir, commitFilename(state.Topic, state.PartitionID))
	file, err := os.CreateTemp(s.dir, ".commit-*.tmp")
	if err != nil {
		return fmt.Errorf("create commit-state temporary file: %w", err)
	}
	tempPath := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(tempPath)
	}
	if err := file.Chmod(0600); err != nil {
		cleanup()
		return err
	}
	if _, err := file.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write commit state: %w", err)
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync commit state: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close commit state: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("publish commit state: %w", err)
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("open commit-state directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync commit-state directory: %w", err)
	}
	return nil
}

// UpdateLeader applies a controller-committed partition leader epoch to this
// replica's durable commit state. It preserves the current high watermark and
// refuses stale epochs or a different leader reusing the same epoch.
func (s *CommitStore) UpdateLeader(topic string, partitionID uint32, leaderID string, leaderEpoch, logEnd uint64) (CommitState, error) {
	if s == nil || strings.TrimSpace(topic) == "" || strings.TrimSpace(leaderID) == "" || leaderEpoch == 0 {
		return CommitState{}, ErrInvalidCommitState
	}
	key := commitKey{topic: topic, partitionID: partitionID}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.states[key]
	if !exists {
		return CommitState{}, fmt.Errorf("partition %s:%d has no initialized commit state: %w", topic, partitionID, ErrInvalidCommitState)
	}
	if current.HighWatermark > logEnd {
		return CommitState{}, ErrCommitAheadOfLog
	}
	if leaderEpoch < current.LeaderEpoch {
		return CommitState{}, ErrCommitLeader
	}
	if leaderEpoch == current.LeaderEpoch {
		if leaderID != current.LeaderID {
			return CommitState{}, ErrCommitLeader
		}
		return current, nil
	}
	updated := current
	updated.LeaderID = leaderID
	updated.LeaderEpoch = leaderEpoch
	if err := s.persist(updated); err != nil {
		return CommitState{}, err
	}
	s.states[key] = updated
	return updated, nil
}
