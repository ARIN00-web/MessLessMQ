package controller

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDurableNodePersistsTermAndVoteAcrossRestart(t *testing.T) {
	directory := t.TempDir()
	node, err := OpenDurableNode("a", []string{"b", "c"}, directory)
	if err != nil {
		t.Fatal(err)
	}
	request, err := node.BeginElection()
	if err != nil {
		t.Fatal(err)
	}
	if request.Term != 1 || node.Status().VotedFor != "a" {
		t.Fatalf("unexpected election state: request=%+v status=%+v", request, node.Status())
	}

	restarted, err := OpenDurableNode("a", []string{"c", "b"}, directory)
	if err != nil {
		t.Fatal(err)
	}
	status := restarted.Status()
	if status.Term != 1 || status.VotedFor != "a" || status.Role != RoleFollower {
		t.Fatalf("term/vote not restored safely: %+v", status)
	}
	response, err := restarted.HandleVoteRequest(VoteRequest{
		Term: 1, CandidateID: "b", LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.VoteGranted {
		t.Fatal("node granted a second vote in the same term after restart")
	}
}

func TestDurableNodePersistsCommittedMetadataLog(t *testing.T) {
	directory := t.TempDir()
	node, err := OpenDurableNode("a", []string{"b", "c"}, directory)
	if err != nil {
		t.Fatal(err)
	}
	request, err := node.BeginElection()
	if err != nil {
		t.Fatal(err)
	}
	if err := node.RecordVote("b", VoteResponse{Term: request.Term, VoteGranted: true}); err != nil {
		t.Fatal(err)
	}
	if err := node.WinElection(); err != nil {
		t.Fatal(err)
	}
	entry, err := node.AppendLocal(Command{Type: CommandRegisterBroker, Broker: Broker{ID: "broker-a", Address: "127.0.0.1:9001"}})
	if err != nil {
		t.Fatal(err)
	}
	if entry.Index != 1 {
		t.Fatalf("unexpected log index: %d", entry.Index)
	}
	if err := node.CommitCurrentTermEntry(2); err != nil {
		t.Fatal(err)
	}

	restarted, err := OpenDurableNode("a", []string{"b", "c"}, directory)
	if err != nil {
		t.Fatal(err)
	}
	status := restarted.Status()
	if status.CommitIndex != 1 || status.AppliedIndex != 1 {
		t.Fatalf("commit state not restored: %+v", status)
	}
	if got := restarted.Metadata().Brokers["broker-a"]; got.ID != "broker-a" {
		t.Fatalf("committed metadata not reconstructed: %+v", restarted.Metadata())
	}
	if status.Role != RoleFollower || status.LeaderID != "" {
		t.Fatalf("restart incorrectly restored volatile leader role: %+v", status)
	}
}

func TestDurableNodeRejectsChangedVoterSet(t *testing.T) {
	directory := t.TempDir()
	if _, err := OpenDurableNode("a", []string{"b", "c"}, directory); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableNode("a", []string{"b", "d"}, directory); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected voter-set mismatch error, got %v", err)
	}
}

func TestDurableNodeRejectsCorruptState(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "raft-state.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"node_id":"a","peers":["b"],"current_term":1,"log":[{"index":2,"term":1}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableNode("a", []string{"b"}, directory); !errors.Is(err, ErrLogInvariant) {
		t.Fatalf("expected log invariant error, got %v", err)
	}
}
