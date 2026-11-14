package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

type reassignmentRequest struct {
	Topic       string   `json:"topic"`
	PartitionID uint32   `json:"partition_id"`
	Replicas    []string `json:"replicas"`
}

func (s *Server) handleStageReassignment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request reassignmentRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&request); err != nil {
		http.Error(w, "invalid reassignment request", http.StatusBadRequest)
		return
	}
	assignment, err := s.StageReassignment(r.Context(), request.Topic, request.PartitionID, request.Replicas)
	s.writeReassignmentResponse(w, assignment, err)
}

func (s *Server) handleFinalizeReassignment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request reassignmentRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&request); err != nil {
		http.Error(w, "invalid reassignment request", http.StatusBadRequest)
		return
	}
	assignment, err := s.FinalizeReassignment(r.Context(), request.Topic, request.PartitionID)
	s.writeReassignmentResponse(w, assignment, err)
}

func (s *Server) writeReassignmentResponse(w http.ResponseWriter, assignment controller.PartitionAssignment, err error) {
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, controller.ErrNotLeader) || errors.Is(err, controller.ErrNoQuorum) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(assignment)
}

// StageReassignment commits the union of the existing and target replicas.
// Existing ISR is preserved, so the new copies receive catch-up traffic but
// cannot affect MinISR or become leaders until they are verified in sync.
func (s *Server) StageReassignment(ctx context.Context, topic string, partitionID uint32, targets []string) (controller.PartitionAssignment, error) {
	if s == nil || s.controllerNode == nil || s.controllerRuntime == nil || ctx == nil || len(targets) == 0 {
		return controller.PartitionAssignment{}, controller.ErrInvalidConfig
	}
	current, err := s.reassignmentCurrent(topic, partitionID)
	if err != nil {
		return controller.PartitionAssignment{}, err
	}
	if len(current.ReassignmentTarget) != 0 {
		return controller.PartitionAssignment{}, controller.ErrMetadataConflict
	}
	if err := validateReassignmentTargets(targets, s.controllerNode.Metadata(), current); err != nil {
		return controller.PartitionAssignment{}, err
	}
	union := append([]string(nil), current.Replicas...)
	for _, target := range targets {
		if !containsControllerReplica(union, target) {
			union = append(union, target)
		}
	}
	if current.LeaderEpoch == ^uint64(0) {
		return controller.PartitionAssignment{}, controller.ErrMetadataConflict
	}
	next := current
	next.Replicas, next.ISR = union, append([]string(nil), current.ISR...)
	next.ReassignmentTarget, next.LeaderEpoch = append([]string(nil), targets...), current.LeaderEpoch+1
	if err := s.controllerRuntime.Propose(ctx, controller.Command{Type: controller.CommandAssignPartition, Assignment: next}); err != nil {
		return controller.PartitionAssignment{}, err
	}
	s.applyCommittedControllerMetadata()
	return s.controllerNode.Metadata().Partitions[current.Key], nil
}

// FinalizeReassignment removes old replicas only after every target is part
// of the controller-committed ISR. A changed leader always gets a new epoch.
func (s *Server) FinalizeReassignment(ctx context.Context, topic string, partitionID uint32) (controller.PartitionAssignment, error) {
	if s == nil || s.controllerNode == nil || s.controllerRuntime == nil || ctx == nil {
		return controller.PartitionAssignment{}, controller.ErrInvalidConfig
	}
	current, err := s.reassignmentCurrent(topic, partitionID)
	if err != nil {
		return controller.PartitionAssignment{}, err
	}
	if len(current.ReassignmentTarget) == 0 {
		return controller.PartitionAssignment{}, controller.ErrMetadataConflict
	}
	for _, target := range current.ReassignmentTarget {
		if !containsControllerReplica(current.ISR, target) {
			return controller.PartitionAssignment{}, fmt.Errorf("target %s not in ISR: %w", target, controller.ErrMetadataConflict)
		}
	}
	if current.LeaderEpoch == ^uint64(0) {
		return controller.PartitionAssignment{}, controller.ErrMetadataConflict
	}
	next := current
	next.Replicas, next.ISR = append([]string(nil), current.ReassignmentTarget...), append([]string(nil), current.ReassignmentTarget...)
	next.ReassignmentTarget = nil
	if !containsControllerReplica(next.Replicas, next.Leader) {
		next.Leader = next.Replicas[0]
	}
	next.LeaderEpoch++
	if err := s.controllerRuntime.Propose(ctx, controller.Command{Type: controller.CommandAssignPartition, Assignment: next}); err != nil {
		return controller.PartitionAssignment{}, err
	}
	s.applyCommittedControllerMetadata()
	return s.controllerNode.Metadata().Partitions[current.Key], nil
}

func (s *Server) reassignmentCurrent(topic string, partitionID uint32) (controller.PartitionAssignment, error) {
	if topic == "" {
		return controller.PartitionAssignment{}, controller.ErrInvalidCommand
	}
	snapshot := s.controllerNode.Metadata()
	current, ok := snapshot.Partitions[controller.PartitionKey{Topic: topic, Partition: int(partitionID)}]
	if !ok {
		return controller.PartitionAssignment{}, controller.ErrMetadataConflict
	}
	return current, nil
}

func validateReassignmentTargets(targets []string, metadata controller.MetadataSnapshot, current controller.PartitionAssignment) error {
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if target == "" {
			return controller.ErrInvalidCommand
		}
		if _, duplicate := seen[target]; duplicate {
			return controller.ErrInvalidCommand
		}
		if _, registered := metadata.Brokers[target]; !registered {
			return errors.Join(controller.ErrBrokerNotRegistered, fmt.Errorf("target %s", target))
		}
		seen[target] = struct{}{}
	}
	if len(targets) == 0 || sameReplicaSet(targets, current.Replicas) {
		return controller.ErrMetadataConflict
	}
	return nil
}

func sameReplicaSet(a, b []string) bool {
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
