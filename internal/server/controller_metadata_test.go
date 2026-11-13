package server

import (
	"testing"
)

func TestBuildControllerAssignmentsRotatesLeadersAcrossFixedVoters(t *testing.T) {
	assignments, err := buildControllerAssignments(
		[]TopicDefinition{{Name: "orders", Partitions: 3}},
		nil,
		[]string{"broker-3", "broker-1", "broker-2"},
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantLeaders := []string{"broker-1", "broker-2", "broker-3"}
	for partitionID, wantLeader := range wantLeaders {
		assignment, ok := assignments[partitionKey{Topic: "orders", PartitionID: uint32(partitionID)}]
		if !ok {
			t.Fatalf("missing assignment for orders:%d", partitionID)
		}
		if assignment.LeaderID != wantLeader {
			t.Fatalf("orders:%d leader=%q, want %q", partitionID, assignment.LeaderID, wantLeader)
		}
		if len(assignment.Replicas) != 3 || assignment.Replicas[0] != wantLeader {
			t.Fatalf("orders:%d replicas=%v, expected three replicas beginning with leader %q", partitionID, assignment.Replicas, wantLeader)
		}
		if assignment.LeaderEpoch != 1 {
			t.Fatalf("orders:%d epoch=%d, want initial epoch 1", partitionID, assignment.LeaderEpoch)
		}
	}
}

func TestBuildControllerAssignmentsRejectsMinISRAboveVoterCount(t *testing.T) {
	_, err := buildControllerAssignments(
		[]TopicDefinition{{Name: "orders", Partitions: 1}},
		nil,
		[]string{"broker-1", "broker-2"},
		3,
	)
	if err == nil {
		t.Fatal("expected min ISR greater than voter count to fail")
	}
}

func TestBuildControllerAssignmentsRejectsConfiguredReplicaOutsideVoters(t *testing.T) {
	configured := map[partitionKey]partitionReplicaAssignment{
		{Topic: "orders", PartitionID: 0}: {
			Topic: "orders", PartitionID: 0,
			Replicas: []string{"broker-1", "broker-9"},
			LeaderID: "broker-1", LeaderEpoch: 1,
		},
	}
	_, err := buildControllerAssignments(
		[]TopicDefinition{{Name: "orders", Partitions: 1}},
		configured,
		[]string{"broker-1", "broker-2", "broker-3"},
		2,
	)
	if err == nil {
		t.Fatal("expected replica outside fixed controller voter set to fail")
	}
}
