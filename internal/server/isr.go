package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

// A single missed probe must not alter durability policy.
const replicaISRFailureThreshold = 3

type replicaHealthKey struct {
	partitionKey
	brokerID string
}

type replicaHealthState struct {
	consecutiveFailures int
	lastSuccess         time.Time
}

// reconcilePartitionISR commits only observed, safe ISR changes. A follower
// joins after it reaches the leader's physical end and durable commit marker;
// an ISR member leaves only after repeated failed or incompatible probes.
func (s *Server) reconcilePartitionISR(parent context.Context) error {
	if s == nil || s.controllerNode == nil || s.controllerRuntime == nil || s.members == nil {
		return nil
	}
	if s.controllerNode.Status().Role != controller.RoleLeader {
		return controller.ErrNotLeader
	}
	snapshot := s.controllerNode.Metadata()
	keys := make([]controller.PartitionKey, 0, len(snapshot.Partitions))
	for key := range snapshot.Partitions {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Topic != keys[j].Topic {
			return keys[i].Topic < keys[j].Topic
		}
		return keys[i].Partition < keys[j].Partition
	})
	var failures []error
	for _, key := range keys {
		assignment := snapshot.Partitions[key]
		partition := partitionKey{Topic: key.Topic, PartitionID: uint32(key.Partition)}
		ctx, cancel := context.WithTimeout(parent, partitionProbeTimeout)
		leader, err := s.queryPartitionReplicaStatus(ctx, assignment.Leader, partition)
		cancel()
		if err != nil || !compatibleLeaderStatus(leader, assignment) {
			// The leader-failure reconciler owns leader changes.
			continue
		}
		desired := make([]string, 0, len(assignment.Replicas))
		for _, replicaID := range assignment.Replicas {
			if replicaID == assignment.Leader {
				desired = append(desired, replicaID)
				continue
			}
			ctx, cancel := context.WithTimeout(parent, partitionProbeTimeout)
			status, probeErr := s.queryPartitionReplicaStatus(ctx, replicaID, partition)
			cancel()
			if s.recordReplicaHealth(partition, replicaID, containsReplica(assignment.ISR, replicaID), probeErr == nil && compatibleFollowerStatus(status, leader, assignment)) {
				desired = append(desired, replicaID)
			}
		}
		if sameStringSlice(desired, assignment.ISR) {
			continue
		}
		proposalCtx, proposalCancel := context.WithTimeout(parent, 5*time.Second)
		err = s.controllerRuntime.Propose(proposalCtx, controller.Command{Type: controller.CommandUpdatePartitionISR, Assignment: controller.PartitionAssignment{
			Key: key, Leader: assignment.Leader, LeaderEpoch: assignment.LeaderEpoch, ISR: desired,
		}})
		proposalCancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("commit ISR for %s:%d: %w", key.Topic, key.Partition, err))
			continue
		}
		s.applyCommittedControllerMetadata()
	}
	return errors.Join(failures...)
}

func compatibleLeaderStatus(status replicationStatusResponse, assignment controller.PartitionAssignment) bool {
	return status.BrokerID == assignment.Leader && status.CommitLeaderID == assignment.Leader && status.LeaderEpoch == assignment.LeaderEpoch && status.HighWatermark <= status.NextOffset && status.StartOffset <= status.HighWatermark
}

func compatibleFollowerStatus(follower, leader replicationStatusResponse, assignment controller.PartitionAssignment) bool {
	return follower.BrokerID != "" && follower.CommitLeaderID == assignment.Leader && follower.LeaderEpoch == assignment.LeaderEpoch && follower.HighWatermark == leader.HighWatermark && follower.NextOffset == leader.NextOffset && follower.StartOffset <= leader.StartOffset
}

// Existing ISR members tolerate brief failures. A replica outside ISR must
// first pass a compatible probe; a timeout can never expand ISR.
func (s *Server) recordReplicaHealth(partition partitionKey, brokerID string, wasInISR, inSync bool) bool {
	s.isrMu.Lock()
	defer s.isrMu.Unlock()
	key := replicaHealthKey{partitionKey: partition, brokerID: brokerID}
	state := s.replicaHealth[key]
	if inSync {
		state.consecutiveFailures = 0
		state.lastSuccess = time.Now()
		s.replicaHealth[key] = state
		return true
	}
	state.consecutiveFailures++
	s.replicaHealth[key] = state
	return wasInISR && state.consecutiveFailures < replicaISRFailureThreshold
}

func sameStringSlice(a, b []string) bool {
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
