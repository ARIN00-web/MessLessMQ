package server

import (
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

func TestCompatibleFollowerStatusRequiresExactCommittedLog(t *testing.T) {
	assignment := controller.PartitionAssignment{Leader: "broker-1", LeaderEpoch: 4}
	leader := replicationStatusResponse{BrokerID: "broker-1", CommitLeaderID: "broker-1", LeaderEpoch: 4, StartOffset: 10, NextOffset: 20, HighWatermark: 20}
	follower := replicationStatusResponse{BrokerID: "broker-2", CommitLeaderID: "broker-1", LeaderEpoch: 4, StartOffset: 10, NextOffset: 20, HighWatermark: 20}
	if !compatibleFollowerStatus(follower, leader, assignment) {
		t.Fatal("matching follower was not eligible for ISR")
	}
	follower.NextOffset = 21
	if compatibleFollowerStatus(follower, leader, assignment) {
		t.Fatal("follower with an uncommitted suffix was accepted")
	}
	follower.NextOffset, follower.HighWatermark = 20, 19
	if compatibleFollowerStatus(follower, leader, assignment) {
		t.Fatal("follower with a stale high watermark was accepted")
	}
}

func TestReplicaHealthRequiresThreeFailuresBeforeRemoval(t *testing.T) {
	s := &Server{replicaHealth: make(map[replicaHealthKey]replicaHealthState)}
	key := partitionKey{Topic: "orders", PartitionID: 0}
	if !s.recordReplicaHealth(key, "broker-2", true, false) || !s.recordReplicaHealth(key, "broker-2", true, false) {
		t.Fatal("transient probe failure removed replica from ISR")
	}
	if s.recordReplicaHealth(key, "broker-2", true, false) {
		t.Fatal("third consecutive probe failure did not remove replica")
	}
	if !s.recordReplicaHealth(key, "broker-2", false, true) {
		t.Fatal("compatible recovery did not restore replica")
	}
}

func TestReplicaHealthDoesNotExpandISROnFailedProbe(t *testing.T) {
	s := &Server{replicaHealth: make(map[replicaHealthKey]replicaHealthState)}
	if s.recordReplicaHealth(partitionKey{Topic: "orders", PartitionID: 0}, "broker-2", false, false) {
		t.Fatal("failed probe admitted a replica that was outside ISR")
	}
}
