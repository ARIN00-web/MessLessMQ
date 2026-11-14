package server

import (
	"errors"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/replication"
)

func TestChoosePromotionCandidatePrefersHighestCommittedReplica(t *testing.T) {
	assignment := controller.PartitionAssignment{
		Key:      controller.PartitionKey{Topic: "orders", Partition: 0},
		Replicas: []string{"broker-1", "broker-2", "broker-3"}, Leader: "broker-1", LeaderEpoch: 7,
	}
	candidate, highWatermark, err := choosePromotionCandidate(assignment, []replicaObservation{
		{BrokerID: "broker-2", Status: replicationStatusResponse{BrokerID: "broker-2", StartOffset: 0, NextOffset: 5, HighWatermark: 5, CommitLeaderID: "broker-1", LeaderEpoch: 7}},
		{BrokerID: "broker-3", Status: replicationStatusResponse{BrokerID: "broker-3", StartOffset: 0, NextOffset: 4, HighWatermark: 4, CommitLeaderID: "broker-1", LeaderEpoch: 7}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if candidate != "broker-2" || highWatermark != 5 {
		t.Fatalf("candidate=%q highWatermark=%d, want broker-2/5", candidate, highWatermark)
	}
}

func TestChoosePromotionCandidateRejectsUncommittedSuffix(t *testing.T) {
	assignment := controller.PartitionAssignment{
		Key:      controller.PartitionKey{Topic: "orders", Partition: 0},
		Replicas: []string{"broker-1", "broker-2", "broker-3"}, Leader: "broker-1", LeaderEpoch: 2,
	}
	_, _, err := choosePromotionCandidate(assignment, []replicaObservation{
		{BrokerID: "broker-2", Status: replicationStatusResponse{BrokerID: "broker-2", StartOffset: 0, NextOffset: 6, HighWatermark: 5, CommitLeaderID: "broker-1", LeaderEpoch: 2}},
	})
	if !errors.Is(err, replication.ErrInvalidCommitState) {
		t.Fatalf("error=%v, want ErrInvalidCommitState", err)
	}
}

func TestEligiblePromotionISROmitsUnverifiedReplica(t *testing.T) {
	assignment := controller.PartitionAssignment{Replicas: []string{"broker-1", "broker-2", "broker-3"}, Leader: "broker-1", LeaderEpoch: 2}
	isr := eligiblePromotionISR(assignment, []replicaObservation{
		{BrokerID: "broker-2", Status: replicationStatusResponse{BrokerID: "broker-2", StartOffset: 0, NextOffset: 5, HighWatermark: 5, CommitLeaderID: "broker-1", LeaderEpoch: 2}},
		{BrokerID: "broker-3", Status: replicationStatusResponse{BrokerID: "broker-3", StartOffset: 0, NextOffset: 6, HighWatermark: 5, CommitLeaderID: "broker-1", LeaderEpoch: 2}},
	}, 5)
	if len(isr) != 1 || isr[0] != "broker-2" {
		t.Fatalf("promotion ISR = %v, want [broker-2]", isr)
	}
}

func TestChoosePromotionCandidateRejectsStaleEpochAndNonReplica(t *testing.T) {
	assignment := controller.PartitionAssignment{
		Key:      controller.PartitionKey{Topic: "orders", Partition: 0},
		Replicas: []string{"broker-1", "broker-2", "broker-3"}, Leader: "broker-1", LeaderEpoch: 4,
	}
	_, _, err := choosePromotionCandidate(assignment, []replicaObservation{
		{BrokerID: "broker-2", Status: replicationStatusResponse{BrokerID: "broker-2", StartOffset: 0, NextOffset: 5, HighWatermark: 5, CommitLeaderID: "broker-1", LeaderEpoch: 3}},
		{BrokerID: "broker-4", Status: replicationStatusResponse{BrokerID: "broker-4", StartOffset: 0, NextOffset: 5, HighWatermark: 5, CommitLeaderID: "broker-1", LeaderEpoch: 4}},
	})
	if !errors.Is(err, replication.ErrInvalidCommitState) {
		t.Fatalf("error=%v, want ErrInvalidCommitState", err)
	}
}

func TestRecordPartitionFailureRequiresThresholdAndRetriesWithBackoff(t *testing.T) {
	server := &Server{partitionFailures: make(map[partitionKey]partitionFailureState)}
	key := partitionKey{Topic: "orders", PartitionID: 0}
	now := time.Now()
	for i := 0; i < partitionLeaderFailureThreshold-1; i++ {
		if server.recordPartitionFailure(key, 1, now) {
			t.Fatalf("promotion triggered on failure %d", i+1)
		}
	}
	if !server.recordPartitionFailure(key, 1, now) {
		t.Fatal("promotion was not triggered at failure threshold")
	}
	if server.recordPartitionFailure(key, 1, now.Add(partitionPromotionRetry/2)) {
		t.Fatal("promotion retry ignored backoff")
	}
	if !server.recordPartitionFailure(key, 1, now.Add(partitionPromotionRetry)) {
		t.Fatal("promotion retry did not become eligible after backoff")
	}
}
