package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/membership"
)

func buildControllerAssignments(definitions []TopicDefinition, configured map[partitionKey]partitionReplicaAssignment, voters []string, minISR int) (map[partitionKey]partitionReplicaAssignment, error) {
	if len(configured) > 0 {
		if len(voters) == 0 {
			return configured, nil
		}
		voterSet := make(map[string]struct{}, len(voters))
		for _, voter := range voters {
			voterSet[voter] = struct{}{}
		}
		for key, assignment := range configured {
			for _, replica := range assignment.Replicas {
				if _, exists := voterSet[replica]; !exists {
					return nil, fmt.Errorf("replica %s for %s:%d is not a configured controller voter", replica, key.Topic, key.PartitionID)
				}
			}
		}
		return configured, nil
	}
	if len(voters) == 0 {
		return map[partitionKey]partitionReplicaAssignment{}, nil
	}
	ordered := append([]string(nil), voters...)
	sort.Strings(ordered)
	if minISR > len(ordered) {
		return nil, fmt.Errorf("minimum ISR %d exceeds controller voter/replica count %d", minISR, len(ordered))
	}
	assignments := make(map[partitionKey]partitionReplicaAssignment)
	for _, definition := range definitions {
		for partitionID := uint32(0); partitionID < definition.Partitions; partitionID++ {
			start := int(partitionID % uint32(len(ordered)))
			replicas := make([]string, 0, len(ordered))
			for offset := range ordered {
				replicas = append(replicas, ordered[(start+offset)%len(ordered)])
			}
			key := partitionKey{Topic: definition.Name, PartitionID: partitionID}
			assignments[key] = partitionReplicaAssignment{
				Topic: definition.Name, PartitionID: partitionID,
				Replicas: replicas, ISR: append([]string(nil), replicas...), LeaderID: replicas[0], LeaderEpoch: 1,
			}
		}
	}
	return assignments, nil
}

func assignmentMatches(current controller.PartitionAssignment, desired partitionReplicaAssignment) bool {
	if current.Key.Topic != desired.Topic || current.Key.Partition != int(desired.PartitionID) ||
		current.Leader != desired.LeaderID || current.LeaderEpoch != desired.LeaderEpoch ||
		len(current.Replicas) != len(desired.Replicas) {
		return false
	}
	for i := range current.Replicas {
		if current.Replicas[i] != desired.Replicas[i] {
			return false
		}
	}
	return true
}

func sortedAssignmentKeys(assignments map[partitionKey]partitionReplicaAssignment) []partitionKey {
	keys := make([]partitionKey, 0, len(assignments))
	for key := range assignments {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Topic != keys[j].Topic {
			return keys[i].Topic < keys[j].Topic
		}
		return keys[i].PartitionID < keys[j].PartitionID
	})
	return keys
}

func (s *Server) runControllerMetadata(ctx context.Context) {
	if s == nil || s.controllerNode == nil || s.controllerRuntime == nil || s.members == nil {
		return
	}
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.applyCommittedControllerMetadata()
			if !s.controllerRuntime.votersReady() || s.controllerNode.Status().Role != controller.RoleLeader {
				continue
			}
			if err := s.reconcileControllerMetadata(ctx); err != nil && !errors.Is(err, controller.ErrNotLeader) && !errors.Is(err, controller.ErrNoQuorum) && !errors.Is(err, context.Canceled) {
				s.logger.Warn("controller metadata reconciliation deferred", "error", err)
			}
			if err := s.reconcilePartitionLeaders(ctx); err != nil && !errors.Is(err, controller.ErrNotLeader) && !errors.Is(err, controller.ErrNoQuorum) && !errors.Is(err, context.Canceled) {
				s.logger.Warn("partition leader reconciliation deferred", "error", err)
			}
			if err := s.reconcilePartitionISR(ctx); err != nil && !errors.Is(err, controller.ErrNotLeader) && !errors.Is(err, controller.ErrNoQuorum) && !errors.Is(err, context.Canceled) {
				s.logger.Warn("partition ISR reconciliation deferred", "error", err)
			}
		}
	}
}

func (s *Server) reconcileControllerMetadata(parent context.Context) error {
	if s.controllerNode.Status().Role != controller.RoleLeader {
		return controller.ErrNotLeader
	}
	membershipSnapshot := s.members.Snapshot()
	brokers := append([]membership.Broker(nil), membershipSnapshot.Brokers...)
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })
	metadata := s.controllerNode.Metadata()

	for _, member := range brokers {
		if existing, exists := metadata.Brokers[member.ID]; exists {
			if existing.Address != member.ClientAddress {
				return fmt.Errorf("broker %s changed advertised client address from %s to %s; update requires an explicit metadata reassignment", member.ID, existing.Address, member.ClientAddress)
			}
			continue
		}
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		err := s.controllerRuntime.Propose(ctx, controller.Command{
			Type:   controller.CommandRegisterBroker,
			Broker: controller.Broker{ID: member.ID, Address: member.ClientAddress},
		})
		cancel()
		if err != nil {
			return fmt.Errorf("commit broker registration for %s: %w", member.ID, err)
		}
		metadata = s.controllerNode.Metadata()
	}

	for _, key := range sortedAssignmentKeys(s.desiredAssignments) {
		desired := s.desiredAssignments[key]
		ready := true
		for _, replica := range desired.Replicas {
			if _, exists := metadata.Brokers[replica]; !exists {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		controllerKey := controller.PartitionKey{Topic: key.Topic, Partition: int(key.PartitionID)}
		if existing, exists := metadata.Partitions[controllerKey]; exists {
			if !assignmentMatches(existing, desired) {
				// Existing committed metadata is authoritative. Reassignment uses a
				// separate staged workflow; startup reconciliation never overwrites it.
				return fmt.Errorf("committed assignment for %s:%d differs from configured desired assignment", key.Topic, key.PartitionID)
			}
			continue
		}
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		err := s.controllerRuntime.Propose(ctx, controller.Command{
			Type: controller.CommandAssignPartition,
			Assignment: controller.PartitionAssignment{
				Key: controllerKey, Replicas: append([]string(nil), desired.Replicas...),
				ISR:    append([]string(nil), desired.ISR...),
				Leader: desired.LeaderID, LeaderEpoch: desired.LeaderEpoch,
			},
		})
		cancel()
		if err != nil {
			return fmt.Errorf("commit partition assignment for %s:%d: %w", key.Topic, key.PartitionID, err)
		}
		metadata = s.controllerNode.Metadata()
	}
	return nil
}

func (s *Server) applyCommittedControllerMetadata() {
	if s == nil || s.controllerNode == nil || s.assignmentStore == nil || s.commits == nil {
		return
	}
	snapshot := s.controllerNode.Metadata()
	assignments := make(map[partitionKey]partitionReplicaAssignment, len(snapshot.Partitions))
	for key, committed := range snapshot.Partitions {
		localTopic, err := s.registry.Get(key.Topic)
		if err != nil {
			continue
		}
		partition, err := localTopic.Partition(uint32(key.Partition))
		if err != nil {
			continue
		}
		assignment := partitionReplicaAssignment{
			Topic: key.Topic, PartitionID: uint32(key.Partition),
			Replicas: append([]string(nil), committed.Replicas...),
			ISR:      append([]string(nil), committed.ISR...),
			LeaderID: committed.Leader, LeaderEpoch: committed.LeaderEpoch,
		}
		if len(assignment.ISR) == 0 {
			assignment.ISR = append([]string(nil), assignment.Replicas...)
		}
		if containsReplica(assignment.Replicas, s.cfg.BrokerID) {
			_, logEnd, rangeErr := partition.OffsetRange()
			if rangeErr != nil {
				s.logger.Warn("cannot inspect partition before applying controller assignment", "topic", key.Topic, "partition", key.Partition, "error", rangeErr)
				continue
			}
			if _, updateErr := s.commits.UpdateLeader(key.Topic, uint32(key.Partition), assignment.LeaderID, assignment.LeaderEpoch, logEnd); updateErr != nil {
				s.logger.Warn("cannot apply controller partition leader locally", "topic", key.Topic, "partition", key.Partition, "leader", assignment.LeaderID, "epoch", assignment.LeaderEpoch, "error", updateErr)
				continue
			}
		}
		assignments[partitionKey{Topic: key.Topic, PartitionID: uint32(key.Partition)}] = assignment
	}
	s.assignmentStore.replace(assignments)
}
