package controller

import "testing"

func TestSyncFollowerReconcilesUncommittedTrailingSuffix(t *testing.T) {
	cluster, err := NewCluster("controller-a", "controller-b", "controller-c")
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Campaign("controller-a"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Commit(Command{
		Type:   CommandRegisterBroker,
		Broker: Broker{ID: "broker-a", Address: "127.0.0.1:9001"},
	}); err != nil {
		t.Fatal(err)
	}

	follower, err := cluster.Node("controller-c")
	if err != nil {
		t.Fatal(err)
	}
	follower.mu.Lock()
	follower.log = append(follower.log, LogEntry{
		Index: 2,
		Term:  1,
		Command: Command{Type: CommandRegisterBroker, Broker: Broker{
			ID: "stale-broker", Address: "127.0.0.1:9999",
		}},
	})
	follower.mu.Unlock()

	if err := cluster.SyncFollower("controller-c"); err != nil {
		t.Fatal(err)
	}
	status := follower.Status()
	if status.LastLogIndex != 1 || status.CommitIndex != 1 || status.AppliedIndex != 1 {
		t.Fatalf("follower did not converge: %+v", status)
	}
	metadata := follower.Metadata()
	if _, exists := metadata.Brokers["stale-broker"]; exists {
		t.Fatal("stale uncommitted broker appeared in metadata")
	}
}
