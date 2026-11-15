package controller

import (
	"errors"
	"testing"
)

func mustCluster(t *testing.T, ids ...string) *Cluster {
	t.Helper()
	cluster, err := NewCluster(ids...)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	return cluster
}

func register(id string) Command {
	return Command{Type: CommandRegisterBroker, Broker: Broker{ID: id, Address: "127.0.0.1:9000"}}
}

func TestAppendRequestFromBuildsIncrementalSuffix(t *testing.T) {
	node, err := newNode("leader", []string{"follower"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.BeginElection(); err != nil {
		t.Fatal(err)
	}
	if err := node.recordVote("follower", VoteResponse{Term: 1, VoteGranted: true}); err != nil {
		t.Fatal(err)
	}
	if err := node.winElection(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"broker-1", "broker-2", "broker-3"} {
		if _, err := node.appendLocal(register(id)); err != nil {
			t.Fatal(err)
		}
	}
	request := node.appendRequestFrom(3)
	if request.PreviousLogIndex != 2 || request.PreviousLogTerm != 1 || len(request.Entries) != 1 {
		t.Fatalf("incremental append request = %+v", request)
	}
	if request.Entries[0].Index != 3 || request.Entries[0].Command.Broker.ID != "broker-3" {
		t.Fatalf("unexpected incremental suffix: %+v", request.Entries)
	}
	last := node.appendRequestFrom(99)
	if last.PreviousLogIndex != 3 || len(last.Entries) != 0 {
		t.Fatalf("heartbeat at end of log = %+v", last)
	}
}

func TestElectionRequiresMajority(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")
	if err := cluster.SetLink("a", "b", false); err != nil {
		t.Fatal(err)
	}
	if err := cluster.SetLink("a", "c", false); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Campaign("a"); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("Campaign error = %v, want ErrNoQuorum", err)
	}
	if got := cluster.CurrentLeader(); got != "" {
		t.Fatalf("leader = %q, want none", got)
	}
}

func TestElectionAndMetadataCommitReplicateToMajority(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")
	if err := cluster.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if got := cluster.CurrentLeader(); got != "a" {
		t.Fatalf("leader = %q, want a", got)
	}
	if err := cluster.Commit(register("broker-1")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		metadata, err := cluster.Metadata(id)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := metadata.Brokers["broker-1"]; !ok {
			t.Fatalf("broker metadata missing on %s", id)
		}
		status := mustNodeStatus(t, cluster, id)
		if status.CommitIndex != 1 || status.AppliedIndex != 1 {
			t.Fatalf("%s commit/applied = %d/%d, want 1/1", id, status.CommitIndex, status.AppliedIndex)
		}
	}
}

func mustNodeStatus(t *testing.T, cluster *Cluster, id string) NodeStatus {
	t.Helper()
	node, err := cluster.Node(id)
	if err != nil {
		t.Fatal(err)
	}
	return node.Status()
}

func TestNodeVotesOnlyOncePerTerm(t *testing.T) {
	node, err := newNode("voter", []string{"one", "two"})
	if err != nil {
		t.Fatal(err)
	}
	first := node.HandleVoteRequest(VoteRequest{Term: 1, CandidateID: "one"})
	if !first.VoteGranted {
		t.Fatal("first vote was not granted")
	}
	second := node.HandleVoteRequest(VoteRequest{Term: 1, CandidateID: "two"})
	if second.VoteGranted {
		t.Fatal("node granted two votes in one term")
	}
	third := node.HandleVoteRequest(VoteRequest{Term: 2, CandidateID: "two"})
	if !third.VoteGranted {
		t.Fatal("node did not grant vote in a newer term")
	}
}

func TestVoteRejectsCandidateWithOutdatedLog(t *testing.T) {
	node, _ := newNode("voter", []string{"candidate", "other", "leader"})
	appendResponse := node.HandleAppendEntries(AppendEntriesRequest{
		Term: 1, LeaderID: "leader",
		Entries: []LogEntry{{Index: 1, Term: 1, Command: register("broker-1")}},
	})
	if !appendResponse.Success {
		t.Fatal("could not seed voter log")
	}
	response := node.HandleVoteRequest(VoteRequest{Term: 2, CandidateID: "candidate", LastLogIndex: 0, LastLogTerm: 0})
	if response.VoteGranted {
		t.Fatal("outdated candidate received a vote")
	}
}

func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	node, _ := newNode("follower", []string{"leader", "other"})
	newer := node.HandleAppendEntries(AppendEntriesRequest{Term: 4, LeaderID: "leader"})
	if !newer.Success || newer.Term != 4 {
		t.Fatalf("newer-term heartbeat response = %+v", newer)
	}
	stale := node.HandleAppendEntries(AppendEntriesRequest{Term: 3, LeaderID: "other"})
	if stale.Success || stale.Term != 4 {
		t.Fatalf("stale-term response = %+v", stale)
	}
}

func TestAppendEntriesTruncatesConflictingUncommittedSuffix(t *testing.T) {
	node, _ := newNode("follower", []string{"leader", "other"})
	first := node.HandleAppendEntries(AppendEntriesRequest{
		Term: 1, LeaderID: "leader",
		Entries: []LogEntry{{Index: 1, Term: 1, Command: register("old")}},
	})
	if !first.Success {
		t.Fatal("initial append failed")
	}
	second := node.HandleAppendEntries(AppendEntriesRequest{
		Term: 2, LeaderID: "other",
		Entries: []LogEntry{{Index: 1, Term: 2, Command: register("new")}},
	})
	if !second.Success {
		t.Fatal("conflicting append failed")
	}
	log := node.Log()
	if len(log) != 1 || log[0].Term != 2 || log[0].Command.Broker.ID != "new" {
		t.Fatalf("log after conflict resolution = %+v", log)
	}
}

func TestMinorityCannotCommitAndLaterCurrentTermEntryCommitsSuffix(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")
	if err := cluster.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	_ = cluster.SetLink("a", "b", false)
	_ = cluster.SetLink("a", "c", false)
	if err := cluster.Commit(register("broker-old")); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("Commit error = %v, want ErrNoQuorum", err)
	}
	metadata, _ := cluster.Metadata("a")
	if _, exists := metadata.Brokers["broker-old"]; exists {
		t.Fatal("uncommitted metadata became visible")
	}
	if got := mustNodeStatus(t, cluster, "a").CommitIndex; got != 0 {
		t.Fatalf("commit index = %d, want 0", got)
	}

	_ = cluster.SetLink("a", "b", true)
	if err := cluster.Commit(register("broker-new")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		metadata, _ = cluster.Metadata(id)
		if _, ok := metadata.Brokers["broker-old"]; !ok {
			t.Fatalf("prior log suffix not committed on %s", id)
		}
		if _, ok := metadata.Brokers["broker-new"]; !ok {
			t.Fatalf("new metadata missing on %s", id)
		}
	}
}

func TestUncommittedCommandIsValidatedBeforeAppendingAnother(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")
	if err := cluster.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	_ = cluster.SetLink("a", "b", false)
	_ = cluster.SetLink("a", "c", false)
	if err := cluster.Commit(register("same")); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("Commit error = %v, want ErrNoQuorum", err)
	}
	if err := cluster.Commit(register("same")); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second Commit error = %v, want ErrAlreadyExists", err)
	}
}

func TestLeaderFailureAndReelection(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")
	if err := cluster.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Commit(register("broker-1")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.SetAvailable("a", false); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Campaign("b"); err != nil {
		t.Fatal(err)
	}
	if got := cluster.CurrentLeader(); got != "b" {
		t.Fatalf("leader after reelection = %q, want b", got)
	}
	if err := cluster.Commit(register("broker-2")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"b", "c"} {
		metadata, _ := cluster.Metadata(id)
		if len(metadata.Brokers) != 2 {
			t.Fatalf("metadata broker count on %s = %d, want 2", id, len(metadata.Brokers))
		}
	}
}

func TestPartitionAssignmentRequiresRegisteredReplicas(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")
	if err := cluster.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	command := Command{Type: CommandAssignPartition, Assignment: PartitionAssignment{
		Key: PartitionKey{Topic: "orders", Partition: 0}, Replicas: []string{"broker-1"},
		Leader: "broker-1", LeaderEpoch: 1,
	}}
	if err := cluster.Commit(command); !errors.Is(err, ErrBrokerNotRegistered) {
		t.Fatalf("Commit error = %v, want ErrBrokerNotRegistered", err)
	}
	if err := cluster.Commit(register("broker-1")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Commit(command); err != nil {
		t.Fatal(err)
	}
	metadata, _ := cluster.Metadata("b")
	assignment, ok := metadata.Partitions[PartitionKey{Topic: "orders", Partition: 0}]
	if !ok || assignment.Leader != "broker-1" || assignment.LeaderEpoch != 1 {
		t.Fatalf("partition assignment = %+v, exists=%v", assignment, ok)
	}
}

func TestPartitionAssignmentPersistsReassignmentTarget(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")
	if err := cluster.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"broker-1", "broker-2"} {
		if err := cluster.Commit(register(id)); err != nil {
			t.Fatal(err)
		}
	}
	command := Command{Type: CommandAssignPartition, Assignment: PartitionAssignment{Key: PartitionKey{Topic: "orders", Partition: 0}, Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1"}, ReassignmentTarget: []string{"broker-2"}, Leader: "broker-1", LeaderEpoch: 1}}
	if err := cluster.Commit(command); err != nil {
		t.Fatal(err)
	}
	metadata, err := cluster.Metadata("b")
	if err != nil {
		t.Fatal(err)
	}
	assignment := metadata.Partitions[command.Assignment.Key]
	if len(assignment.ReassignmentTarget) != 1 || assignment.ReassignmentTarget[0] != "broker-2" {
		t.Fatalf("target = %v", assignment.ReassignmentTarget)
	}
}

func TestAppendEntriesCannotOverwriteCommittedEntry(t *testing.T) {
	node, _ := newNode("follower", []string{"leader", "other"})
	seed := node.HandleAppendEntries(AppendEntriesRequest{
		Term: 1, LeaderID: "leader",
		Entries:           []LogEntry{{Index: 1, Term: 1, Command: register("committed")}},
		LeaderCommitIndex: 1,
	})
	if !seed.Success {
		t.Fatal("initial committed append failed")
	}
	conflict := node.HandleAppendEntries(AppendEntriesRequest{
		Term: 2, LeaderID: "other",
		Entries: []LogEntry{{Index: 1, Term: 2, Command: register("replacement")}},
	})
	if conflict.Success {
		t.Fatal("AppendEntries overwrote a committed entry")
	}
	metadata := node.Metadata()
	if _, ok := metadata.Brokers["committed"]; !ok {
		t.Fatal("committed metadata was lost")
	}
}

func TestSingleNodeClusterCanElectAndCommit(t *testing.T) {
	cluster := mustCluster(t, "solo")
	if err := cluster.Campaign("solo"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Commit(register("broker-1")); err != nil {
		t.Fatal(err)
	}
	metadata, _ := cluster.Metadata("solo")
	if _, ok := metadata.Brokers["broker-1"]; !ok {
		t.Fatal("single-node metadata command was not committed")
	}
}

func TestUnknownCandidateCannotReceiveVote(t *testing.T) {
	node, _ := newNode("voter", []string{"known"})
	response := node.HandleVoteRequest(VoteRequest{Term: 1, CandidateID: "stranger"})
	if response.VoteGranted {
		t.Fatal("unknown candidate received a vote")
	}
}

func TestMetadataSnapshotsAreDetached(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")
	if err := cluster.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Commit(register("broker-1")); err != nil {
		t.Fatal(err)
	}
	metadata, _ := cluster.Metadata("a")
	delete(metadata.Brokers, "broker-1")
	again, _ := cluster.Metadata("a")
	if _, ok := again.Brokers["broker-1"]; !ok {
		t.Fatal("caller mutation leaked into controller metadata")
	}
}

func TestNewClusterRejectsInvalidMembership(t *testing.T) {
	for _, ids := range [][]string{nil, {"a", "a"}, {""}} {
		if _, err := NewCluster(ids...); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewCluster(%v) error = %v, want ErrInvalidConfig", ids, err)
		}
	}
}

func TestBrokerAddressUpdate(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")

	if err := cluster.Campaign("a"); err != nil {
		t.Fatal(err)
	}

	if err := cluster.Commit(register("broker-1")); err != nil {
		t.Fatal(err)
	}

	update := Command{
		Type: CommandUpdateBrokerAddress,
		Broker: Broker{
			ID:      "broker-1",
			Address: "127.0.0.1:19092",
		},
	}

	if err := cluster.Commit(update); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"a", "b", "c"} {
		metadata, err := cluster.Metadata(id)
		if err != nil {
			t.Fatal(err)
		}

		broker, exists := metadata.Brokers["broker-1"]
		if !exists {
			t.Fatalf("broker-1 missing from metadata on %s", id)
		}

		if broker.Address != "127.0.0.1:19092" {
			t.Fatalf("broker address on %s = %q, want 127.0.0.1:19092",
				id, broker.Address)
		}
	}
}

func TestBrokerAddressUpdateRequiresRegisteredBroker(t *testing.T) {
	cluster := mustCluster(t, "a", "b", "c")

	if err := cluster.Campaign("a"); err != nil {
		t.Fatal(err)
	}

	err := cluster.Commit(Command{
		Type: CommandUpdateBrokerAddress,
		Broker: Broker{
			ID:      "unknown",
			Address: "127.0.0.1:19092",
		},
	})

	if !errors.Is(err, ErrBrokerNotRegistered) {
		t.Fatalf("Commit error = %v, want ErrBrokerNotRegistered", err)
	}
}

func TestNetworkPartitionAndMajorityConvergence(t *testing.T) {
	// 5-node cluster: a, b, c, d, e. Majority is 3.
	cluster := mustCluster(t, "a", "b", "c", "d", "e")

	// Elect "a" as initial leader.
	if err := cluster.Campaign("a"); err != nil {
		t.Fatalf("initial campaign failed: %v", err)
	}

	// Commit initial command on leader "a".
	if err := cluster.Commit(register("broker-1")); err != nil {
		t.Fatalf("initial commit failed: %v", err)
	}

	// Partition the network into minority {a, b} and majority {c, d, e}.
	minority := []string{"a", "b"}
	majority := []string{"c", "d", "e"}
	for _, minNode := range minority {
		for _, majNode := range majority {
			if err := cluster.SetLink(minNode, majNode, false); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Proposals on minority {a, b} must fail due to lack of quorum.
	if err := cluster.Commit(register("broker-stale")); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("minority commit error = %v, want ErrNoQuorum", err)
	}

	// Majority {c, d, e} can campaign and elect "c".
	if err := cluster.Campaign("c"); err != nil {
		t.Fatalf("majority campaign for c failed: %v", err)
	}
	if got := cluster.CurrentLeader(); got != "c" {
		t.Fatalf("current leader = %q, want c", got)
	}

	// Leader "c" can commit new commands within majority quorum.
	if err := cluster.Commit(register("broker-2")); err != nil {
		t.Fatalf("majority commit broker-2 failed: %v", err)
	}
	if err := cluster.Commit(register("broker-3")); err != nil {
		t.Fatalf("majority commit broker-3 failed: %v", err)
	}

	// Heal the network partition.
	for _, minNode := range minority {
		for _, majNode := range majority {
			if err := cluster.SetLink(minNode, majNode, true); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Sync surviving followers "a" and "b" from new leader "c".
	if err := cluster.SyncFollower("a"); err != nil {
		t.Fatalf("sync follower a failed: %v", err)
	}
	if err := cluster.SyncFollower("b"); err != nil {
		t.Fatalf("sync follower b failed: %v", err)
	}

	// Verify all 5 nodes converged to the exact same metadata and log state.
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		metadata, err := cluster.Metadata(id)
		if err != nil {
			t.Fatalf("Metadata(%s) failed: %v", id, err)
		}
		if len(metadata.Brokers) != 3 {
			t.Fatalf("node %s has %d brokers, want 3", id, len(metadata.Brokers))
		}
		if _, ok := metadata.Brokers["broker-stale"]; ok {
			t.Fatalf("node %s has uncommitted broker-stale in metadata", id)
		}
		for _, brokerID := range []string{"broker-1", "broker-2", "broker-3"} {
			if _, ok := metadata.Brokers[brokerID]; !ok {
				t.Fatalf("node %s missing %s in metadata", id, brokerID)
			}
		}
		status := mustNodeStatus(t, cluster, id)
		if status.CommitIndex != 3 || status.AppliedIndex != 3 {
			t.Fatalf("node %s commit/applied = %d/%d, want 3/3", id, status.CommitIndex, status.AppliedIndex)
		}
	}
}
