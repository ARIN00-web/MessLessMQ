package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/interbroker"
	"github.com/ARIN00-web/messlessmq/internal/membership"
	"github.com/ARIN00-web/messlessmq/internal/replication"
)

const (
	partitionLeaderFailureThreshold = 3
	partitionProbeTimeout           = 350 * time.Millisecond
	partitionPromotionRetry         = time.Second
)

type partitionFailureState struct {
	LeaderEpoch uint64
	Misses      int
	LastAttempt time.Time
}

type partitionStatusRequest struct {
	Topic       string `json:"topic"`
	PartitionID uint32 `json:"partition_id"`
}

// reconcilePartitionLeaders is run only by the elected controller leader. A
// partition leader is replaced only when it is unreachable and an assigned
// replica proves that its physical log ends exactly at its durable high
// watermark. This deliberately refuses promotion when a candidate contains an
// uncommitted suffix: storage truncation/rebootstrap is a separate operation.
func (s *Server) reconcilePartitionLeaders(parent context.Context) error {
	if s == nil || s.controllerNode == nil || s.controllerRuntime == nil || s.members == nil {
		return nil
	}
	if s.controllerNode.Status().Role != controller.RoleLeader {
		return controller.ErrNotLeader
	}
	snapshot := s.controllerNode.Metadata()
	keys := make([]partitionKey, 0, len(snapshot.Partitions))
	for key := range snapshot.Partitions {
		keys = append(keys, partitionKey{Topic: key.Topic, PartitionID: uint32(key.Partition)})
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Topic != keys[j].Topic {
			return keys[i].Topic < keys[j].Topic
		}
		return keys[i].PartitionID < keys[j].PartitionID
	})

	var failures []error
	for _, key := range keys {
		if err := parent.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		assignment := snapshot.Partitions[controller.PartitionKey{Topic: key.Topic, Partition: int(key.PartitionID)}]
		if assignment.Leader == "" || assignment.Leader == s.cfg.BrokerID {
			s.resetPartitionFailure(key)
			continue
		}
		ctx, cancel := context.WithTimeout(parent, partitionProbeTimeout)
		_, probeErr := s.queryPartitionReplicaStatus(ctx, assignment.Leader, key)
		cancel()
		if probeErr == nil {
			s.resetPartitionFailure(key)
			continue
		}
		if !s.recordPartitionFailure(key, assignment.LeaderEpoch, time.Now()) {
			continue
		}
		if s.controllerNode.Status().Role != controller.RoleLeader {
			return controller.ErrNotLeader
		}
		candidate, highWatermark, promotionISR, err := s.selectPromotionCandidate(parent, key, assignment)
		if err != nil {
			failures = append(failures, fmt.Errorf("select replacement for %s:%d after leader %s became unreachable: %w", key.Topic, key.PartitionID, assignment.Leader, err))
			continue
		}
		current := s.controllerNode.Metadata().Partitions[controller.PartitionKey{Topic: key.Topic, Partition: int(key.PartitionID)}]
		if current.Leader != assignment.Leader || current.LeaderEpoch != assignment.LeaderEpoch {
			s.resetPartitionFailure(key)
			continue
		}
		if s.controllerNode.Status().Role != controller.RoleLeader {
			return controller.ErrNotLeader
		}
		command := controller.Command{Type: controller.CommandAssignPartition, Assignment: controller.PartitionAssignment{
			Key: current.Key, Replicas: append([]string(nil), current.Replicas...),
			// Carry forward only replicas observed safe during this promotion.
			// Never inherit the previous ISR blindly across a leader epoch.
			ISR:    promotionISR,
			Leader: candidate, LeaderEpoch: current.LeaderEpoch + 1,
		}}
		proposeCtx, proposeCancel := context.WithTimeout(parent, 5*time.Second)
		err = s.controllerRuntime.Propose(proposeCtx, command)
		proposeCancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("commit partition leader promotion for %s:%d to %s: %w", key.Topic, key.PartitionID, candidate, err))
			continue
		}
		s.applyCommittedControllerMetadata()
		s.resetPartitionFailure(key)
		s.logger.Warn("partition leader promoted", "topic", key.Topic, "partition", key.PartitionID, "old_leader", assignment.Leader, "new_leader", candidate, "leader_epoch", current.LeaderEpoch+1, "high_watermark", highWatermark)
	}
	return errors.Join(failures...)
}

func (s *Server) recordPartitionFailure(key partitionKey, epoch uint64, now time.Time) bool {
	s.failoverMu.Lock()
	defer s.failoverMu.Unlock()
	if s.partitionFailures == nil {
		s.partitionFailures = make(map[partitionKey]partitionFailureState)
	}
	state := s.partitionFailures[key]
	if state.LeaderEpoch != epoch {
		state = partitionFailureState{LeaderEpoch: epoch}
	}
	state.Misses++
	if state.Misses < partitionLeaderFailureThreshold {
		s.partitionFailures[key] = state
		return false
	}
	if !state.LastAttempt.IsZero() && now.Sub(state.LastAttempt) < partitionPromotionRetry {
		s.partitionFailures[key] = state
		return false
	}
	state.LastAttempt = now
	s.partitionFailures[key] = state
	return true
}

func (s *Server) resetPartitionFailure(key partitionKey) {
	s.failoverMu.Lock()
	delete(s.partitionFailures, key)
	s.failoverMu.Unlock()
}

type replicaObservation struct {
	BrokerID string
	Status   replicationStatusResponse
}

func choosePromotionCandidate(assignment controller.PartitionAssignment, observed []replicaObservation) (string, uint64, error) {
	var highestCommitted uint64
	valid := make([]replicaObservation, 0, len(observed))
	for _, replica := range observed {
		if replica.BrokerID == "" || replica.BrokerID == assignment.Leader || !containsControllerReplica(assignment.Replicas, replica.BrokerID) {
			continue
		}
		status := replica.Status
		if status.BrokerID != replica.BrokerID || status.CommitLeaderID != assignment.Leader || status.LeaderEpoch != assignment.LeaderEpoch {
			continue
		}
		valid = append(valid, replica)
		if status.HighWatermark > highestCommitted {
			highestCommitted = status.HighWatermark
		}
	}
	candidates := make([]string, 0, len(valid))
	for _, replica := range valid {
		status := replica.Status
		// Do not promote a replica with an uncommitted suffix. The current log
		// format has no safe in-place truncation operation; refusing is safer
		// than assigning leadership over divergent offsets.
		if status.HighWatermark != highestCommitted || status.NextOffset != status.HighWatermark || status.StartOffset > status.HighWatermark {
			continue
		}
		candidates = append(candidates, replica.BrokerID)
	}
	if len(candidates) == 0 {
		return "", highestCommitted, replication.ErrInvalidCommitState
	}
	sort.Strings(candidates)
	return candidates[0], highestCommitted, nil
}

func eligiblePromotionISR(assignment controller.PartitionAssignment, observed []replicaObservation, highWatermark uint64) []string {
	valid := make(map[string]struct{}, len(observed))
	for _, replica := range observed {
		status := replica.Status
		if replica.BrokerID == "" || !containsControllerReplica(assignment.Replicas, replica.BrokerID) ||
			status.BrokerID != replica.BrokerID || status.CommitLeaderID != assignment.Leader || status.LeaderEpoch != assignment.LeaderEpoch ||
			status.HighWatermark != highWatermark || status.NextOffset != highWatermark || status.StartOffset > highWatermark {
			continue
		}
		valid[replica.BrokerID] = struct{}{}
	}
	isr := make([]string, 0, len(valid))
	for _, replicaID := range assignment.Replicas {
		if _, ok := valid[replicaID]; ok {
			isr = append(isr, replicaID)
		}
	}
	return isr
}

func containsControllerReplica(replicas []string, brokerID string) bool {
	for _, replica := range replicas {
		if replica == brokerID {
			return true
		}
	}
	return false
}

func (s *Server) selectPromotionCandidate(parent context.Context, key partitionKey, assignment controller.PartitionAssignment) (string, uint64, []string, error) {
	observed := make([]replicaObservation, 0, len(assignment.Replicas))
	var queryErrors []error
	for _, replicaID := range assignment.Replicas {
		if replicaID == assignment.Leader {
			continue
		}
		ctx, cancel := context.WithTimeout(parent, partitionProbeTimeout)
		status, err := s.queryPartitionReplicaStatus(ctx, replicaID, partitionKey{Topic: key.Topic, PartitionID: key.PartitionID})
		cancel()
		if err != nil {
			queryErrors = append(queryErrors, fmt.Errorf("replica %s: %w", replicaID, err))
			continue
		}
		observed = append(observed, replicaObservation{BrokerID: replicaID, Status: status})
	}
	candidate, highWatermark, err := choosePromotionCandidate(assignment, observed)
	if err != nil {
		return "", highWatermark, nil, errors.Join(append([]error{err}, queryErrors...)...)
	}
	isr := eligiblePromotionISR(assignment, observed, highWatermark)
	if !containsControllerReplica(isr, candidate) {
		return "", highWatermark, nil, replication.ErrInvalidCommitState
	}
	return candidate, highWatermark, isr, nil
}

func (s *Server) queryPartitionReplicaStatus(ctx context.Context, brokerID string, key partitionKey) (replicationStatusResponse, error) {
	if brokerID == s.cfg.BrokerID {
		registeredTopic, err := s.registry.Get(key.Topic)
		if err != nil {
			return replicationStatusResponse{}, err
		}
		partition, err := registeredTopic.Partition(key.PartitionID)
		if err != nil {
			return replicationStatusResponse{}, err
		}
		start, next, err := partition.OffsetRange()
		if err != nil {
			return replicationStatusResponse{}, err
		}
		commitState, ok := s.commits.Get(key.Topic, key.PartitionID)
		if !ok {
			return replicationStatusResponse{}, replication.ErrInvalidCommitState
		}
		return replicationStatusResponse{BrokerID: brokerID, StartOffset: start, NextOffset: next, HighWatermark: commitState.HighWatermark, CommitLeaderID: commitState.LeaderID, LeaderEpoch: commitState.LeaderEpoch}, nil
	}
	peer, ok := s.members.Broker(brokerID)
	if !ok {
		return replicationStatusResponse{}, membership.ErrUnknownPeer
	}
	payload, err := json.Marshal(partitionStatusRequest{Topic: key.Topic, PartitionID: key.PartitionID})
	if err != nil {
		return replicationStatusResponse{}, err
	}
	requestID := partitionStatusRequestSequence.Add(1)
	response, err := partitionStatusTransport.RoundTrip(ctx, peer.PeerAddress, interbroker.Envelope{
		Type: interbroker.MessagePartitionStatus, RequestID: requestID,
		SourceID: s.cfg.BrokerID, TargetID: brokerID, Payload: payload,
	})
	if err != nil {
		return replicationStatusResponse{}, err
	}
	var status replicationStatusResponse
	if err := json.Unmarshal(response.Payload, &status); err != nil {
		return replicationStatusResponse{}, fmt.Errorf("decode partition status from %s: %w", brokerID, err)
	}
	if status.BrokerID != brokerID {
		return replicationStatusResponse{}, fmt.Errorf("partition status identity mismatch: got %s, want %s", status.BrokerID, brokerID)
	}
	return status, nil
}

var partitionStatusTransport = interbroker.NewTransport(partitionProbeTimeout)
var partitionStatusRequestSequence atomic.Uint64
