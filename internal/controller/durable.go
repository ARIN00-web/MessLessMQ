package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const durableStateVersion = 1

// DurableNode wraps the Raft state machine and persists term, vote, log, and
// commit index before a protocol operation reports success. Election role and
// leader identity are intentionally volatile: after restart every node begins
// as a follower and must learn a leader through a fresh AppendEntries RPC.
type DurableNode struct {
	mu   sync.Mutex
	node *Node
	path string
}

type durableNodeState struct {
	Version     uint16     `json:"version"`
	NodeID      string     `json:"node_id"`
	Peers       []string   `json:"peers"`
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    string     `json:"voted_for,omitempty"`
	Log         []LogEntry `json:"log"`
	CommitIndex uint64     `json:"commit_index"`
}

// OpenDurableNode loads or initializes one fixed-membership Raft participant.
// peers must be the same on every member and remain unchanged across restarts.
func OpenDurableNode(id string, peers []string, directory string) (*DurableNode, error) {
	if id == "" || directory == "" {
		return nil, ErrInvalidConfig
	}
	peerCopy := append([]string(nil), peers...)
	sort.Strings(peerCopy)
	node, err := newNode(id, peerCopy)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, fmt.Errorf("create controller state directory: %w", err)
	}
	durable := &DurableNode{node: node, path: filepath.Join(directory, "raft-state.json")}
	data, err := os.ReadFile(durable.path)
	if err == nil {
		var state durableNodeState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, fmt.Errorf("decode durable controller state: %w", err)
		}
		if state.Version != durableStateVersion || state.NodeID != id || !equalStrings(state.Peers, peerCopy) {
			return nil, fmt.Errorf("durable controller identity or voter set changed: %w", ErrInvalidConfig)
		}
		if state.CommitIndex > uint64(len(state.Log)) || state.CurrentTerm == 0 && (state.VotedFor != "" || len(state.Log) != 0) {
			return nil, ErrLogInvariant
		}
		if state.VotedFor != "" && state.VotedFor != id && !containsString(peerCopy, state.VotedFor) {
			return nil, ErrLogInvariant
		}
		var previousTerm uint64
		for index, entry := range state.Log {
			if entry.Index != uint64(index+1) || entry.Term == 0 || entry.Term < previousTerm || entry.Term > state.CurrentTerm {
				return nil, ErrLogInvariant
			}
			if err := validateCommandShape(entry.Command); err != nil {
				return nil, fmt.Errorf("invalid durable controller command at index %d: %w", entry.Index, err)
			}
			previousTerm = entry.Term
		}
		node.currentTerm = state.CurrentTerm
		node.votedFor = state.VotedFor
		node.log = cloneEntries(state.Log)
		node.commitIndex = state.CommitIndex
		node.lastApplied = 0
		node.role = RoleFollower
		node.leaderID = ""
		node.votes = make(map[string]struct{})
		if err := node.applyCommittedLocked(); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read durable controller state: %w", err)
	} else {
		if err := durable.persistLocked(); err != nil {
			return nil, err
		}
	}
	return durable, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (d *DurableNode) persistLocked() error {
	if d == nil || d.node == nil || d.path == "" {
		return ErrInvalidConfig
	}
	d.node.mu.Lock()
	state := durableNodeState{
		Version: durableStateVersion, NodeID: d.node.id,
		Peers: append([]string(nil), d.node.peers...), CurrentTerm: d.node.currentTerm,
		VotedFor: d.node.votedFor, Log: cloneEntries(d.node.log), CommitIndex: d.node.commitIndex,
	}
	d.node.mu.Unlock()
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode durable controller state: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(d.path), ".raft-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create controller state temp file: %w", err)
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
		return fmt.Errorf("write controller state: %w", err)
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync controller state: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close controller state: %w", err)
	}
	if err := os.Rename(tempPath, d.path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("publish controller state: %w", err)
	}
	directory, err := os.Open(filepath.Dir(d.path))
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync controller state directory: %w", err)
	}
	return nil
}

func (d *DurableNode) ID() string                 { return d.node.ID() }
func (d *DurableNode) Status() NodeStatus         { return d.node.Status() }
func (d *DurableNode) Metadata() MetadataSnapshot { return d.node.Metadata() }
func (d *DurableNode) Log() []LogEntry            { return d.node.Log() }

func (d *DurableNode) BeginElection() (VoteRequest, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	request, err := d.node.BeginElection()
	if err != nil {
		return VoteRequest{}, err
	}
	if err := d.persistLocked(); err != nil {
		return VoteRequest{}, err
	}
	return request, nil
}

func (d *DurableNode) HandleVoteRequest(request VoteRequest) (VoteResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	response := d.node.HandleVoteRequest(request)
	if err := d.persistLocked(); err != nil {
		return VoteResponse{}, err
	}
	return response, nil
}

func (d *DurableNode) HandleAppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	response := d.node.HandleAppendEntries(request)
	if err := d.persistLocked(); err != nil {
		return AppendEntriesResponse{}, err
	}
	return response, nil
}

func (d *DurableNode) RecordVote(peerID string, response VoteResponse) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.node.recordVote(peerID, response); err != nil {
		return err
	}
	return d.persistLocked()
}

func (d *DurableNode) WinElection() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.node.winElection()
}

func (d *DurableNode) AppendLocal(command Command) (LogEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, err := d.node.appendLocal(command)
	if err != nil {
		return LogEntry{}, err
	}
	if err := d.persistLocked(); err != nil {
		return LogEntry{}, err
	}
	return entry, nil
}

func (d *DurableNode) AppendRequest() AppendEntriesRequest { return d.node.appendRequest() }

func (d *DurableNode) CommitCurrentTermEntry(replicated int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.node.commitCurrentTermEntry(replicated); err != nil {
		return err
	}
	return d.persistLocked()
}
