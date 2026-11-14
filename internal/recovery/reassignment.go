package recovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/ARIN00-web/messlessmq/internal/cluster"
	"github.com/ARIN00-web/messlessmq/internal/controller"
)

type ReassignmentState uint8

const (
	ReassignmentPlanned ReassignmentState = iota + 1
	ReassignmentCommitting
	ReassignmentCompleted
	ReassignmentAborted
	ReassignmentCommitUncertain
)

func (s ReassignmentState) String() string {
	switch s {
	case ReassignmentPlanned:
		return "planned"
	case ReassignmentCommitting:
		return "committing"
	case ReassignmentCompleted:
		return "completed"
	case ReassignmentAborted:
		return "aborted"
	case ReassignmentCommitUncertain:
		return "commit-uncertain"
	default:
		return "unknown"
	}
}

type ReassignmentPlan struct {
	ID                 string
	Key                controller.PartitionKey
	MetadataVersion    uint64
	OriginalAssignment controller.PartitionAssignment
	TargetReplicas     []string
	SourceLeader       string
	State              ReassignmentState
}

// ReplicaReadiness is evidence collected from the data plane after catch-up.
// MatchOffset and HighWatermark are exclusive next offsets. A target is safe
// to publish only when it is available, belongs to the ISR, and has reached
// at least the source high watermark under the same replication leader epoch.
type ReplicaReadiness struct {
	BrokerID      string
	SourceLeader  string
	LeaderEpoch   uint64
	MatchOffset   uint64
	HighWatermark uint64
	Available     bool
	InSync        bool
}

type ReplicaReadinessProvider interface {
	InspectReplica(topic string, partition uint32, brokerID string) (ReplicaReadiness, error)
}

type ReplicaReadinessFunc func(topic string, partition uint32, brokerID string) (ReplicaReadiness, error)

func (f ReplicaReadinessFunc) InspectReplica(topic string, partition uint32, brokerID string) (ReplicaReadiness, error) {
	return f(topic, partition, brokerID)
}

// ReassignmentCoordinator stages replica-set changes in memory and commits
// the new metadata only after every target replica proves it is caught up.
// Physical copying is delegated to the broker/replication layer.
type ReassignmentCoordinator struct {
	mu       sync.Mutex
	manager  *cluster.Manager
	plans    map[string]ReassignmentPlan
	active   map[controller.PartitionKey]string
	nextPlan uint64
	store    *reassignmentStore
}

func NewReassignmentCoordinator(manager *cluster.Manager) (*ReassignmentCoordinator, error) {
	if manager == nil || manager.ControllerCluster() == nil {
		return nil, ErrInvalidConfig
	}
	return &ReassignmentCoordinator{
		manager: manager,
		plans:   make(map[string]ReassignmentPlan),
		active:  make(map[controller.PartitionKey]string),
	}, nil
}

// OpenReassignmentCoordinator restores durable reassignment plans. A plan
// persisted while its metadata proposal was in flight is marked uncertain:
// callers must inspect committed controller metadata rather than blindly
// retrying a potentially committed assignment.
func OpenReassignmentCoordinator(manager *cluster.Manager, directory string) (*ReassignmentCoordinator, error) {
	if manager == nil || manager.ControllerCluster() == nil || directory == "" {
		return nil, ErrInvalidConfig
	}
	store, state, err := openReassignmentStore(directory)
	if err != nil {
		return nil, err
	}
	coordinator := &ReassignmentCoordinator{manager: manager, plans: make(map[string]ReassignmentPlan), active: make(map[controller.PartitionKey]string), nextPlan: state.NextPlan, store: store}
	for id, plan := range state.Plans {
		if id != plan.ID || plan.ID == "" {
			return nil, fmt.Errorf("invalid persisted reassignment plan: %w", ErrInvalidConfig)
		}
		if plan.State == ReassignmentCommitting {
			plan.State = ReassignmentCommitUncertain
		}
		coordinator.plans[id] = clonePlan(plan)
		if plan.State == ReassignmentPlanned || plan.State == ReassignmentCommitUncertain {
			if _, exists := coordinator.active[plan.Key]; exists {
				return nil, fmt.Errorf("duplicate active reassignment for %s/%d: %w", plan.Key.Topic, plan.Key.Partition, ErrInvalidConfig)
			}
			coordinator.active[plan.Key] = plan.ID
		}
	}
	if err := coordinator.persistLocked(); err != nil {
		return nil, err
	}
	return coordinator, nil
}

func (c *ReassignmentCoordinator) Begin(topic string, partition int, targetReplicas []string) (ReassignmentPlan, error) {
	if c == nil || c.manager == nil || strings.TrimSpace(topic) != topic || topic == "" || partition < 0 || len(targetReplicas) == 0 {
		return ReassignmentPlan{}, ErrInvalidConfig
	}
	leaderID := c.manager.ControllerLeader()
	if leaderID == "" {
		return ReassignmentPlan{}, ErrNoControllerLeader
	}
	snapshot, err := c.manager.Metadata(leaderID)
	if err != nil {
		return ReassignmentPlan{}, err
	}
	key := controller.PartitionKey{Topic: topic, Partition: partition}
	current, exists := snapshot.Partitions[key]
	if !exists {
		return ReassignmentPlan{}, cluster.ErrPartitionUnassigned
	}
	brokers := snapshot.Brokers
	seen := make(map[string]struct{}, len(targetReplicas))
	target := append([]string(nil), targetReplicas...)
	for _, id := range target {
		if strings.TrimSpace(id) != id || id == "" {
			return ReassignmentPlan{}, ErrInvalidConfig
		}
		if _, duplicate := seen[id]; duplicate {
			return ReassignmentPlan{}, ErrInvalidConfig
		}
		if _, registered := brokers[id]; !registered {
			return ReassignmentPlan{}, fmt.Errorf("target broker %q is not registered: %w", id, ErrInvalidConfig)
		}
		seen[id] = struct{}{}
	}
	if slices.Equal(target, current.Replicas) {
		return ReassignmentPlan{}, ErrInvalidConfig
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.active[key]; exists {
		return ReassignmentPlan{}, ErrPlanExists
	}
	c.nextPlan++
	plan := ReassignmentPlan{
		ID:                 fmt.Sprintf("reassign-%d", c.nextPlan),
		Key:                key,
		MetadataVersion:    snapshot.Version,
		OriginalAssignment: cloneAssignment(current),
		TargetReplicas:     target,
		SourceLeader:       current.Leader,
		State:              ReassignmentPlanned,
	}
	c.plans[plan.ID] = clonePlan(plan)
	c.active[key] = plan.ID
	if err := c.persistLocked(); err != nil {
		delete(c.plans, plan.ID)
		delete(c.active, key)
		return ReassignmentPlan{}, err
	}
	return clonePlan(plan), nil
}

// Complete validates all target replicas against one common source leader,
// epoch, and high watermark, then commits the new assignment. If the current
// leader is removed, the first target replica becomes the new leader only
// after it has passed the same readiness check as every other target.
func (c *ReassignmentCoordinator) Complete(planID string, provider ReplicaReadinessProvider) (ReassignmentPlan, error) {
	if c == nil || provider == nil {
		return ReassignmentPlan{}, ErrInvalidConfig
	}
	c.mu.Lock()
	plan, exists := c.plans[planID]
	if !exists {
		c.mu.Unlock()
		return ReassignmentPlan{}, ErrPlanNotFound
	}
	if plan.State != ReassignmentPlanned {
		c.mu.Unlock()
		return ReassignmentPlan{}, ErrPlanState
	}
	plan.State = ReassignmentCommitting
	c.plans[planID] = plan
	if err := c.persistLocked(); err != nil {
		c.mu.Unlock()
		return ReassignmentPlan{}, err
	}
	c.mu.Unlock()

	resetToPlanned := func(err error) (ReassignmentPlan, error) {
		c.mu.Lock()
		if current, ok := c.plans[planID]; ok && current.State == ReassignmentCommitting {
			current.State = ReassignmentPlanned
			c.plans[planID] = current
			_ = c.persistLocked()
		}
		c.mu.Unlock()
		return ReassignmentPlan{}, err
	}
	markUncertain := func(err error) (ReassignmentPlan, error) {
		c.mu.Lock()
		if current, ok := c.plans[planID]; ok {
			current.State = ReassignmentCommitUncertain
			c.plans[planID] = current
			_ = c.persistLocked()
		}
		c.mu.Unlock()
		return ReassignmentPlan{}, err
	}

	leaderID := c.manager.ControllerLeader()
	if leaderID == "" {
		return resetToPlanned(ErrNoControllerLeader)
	}
	snapshot, err := c.manager.Metadata(leaderID)
	if err != nil {
		return resetToPlanned(err)
	}
	current, exists := snapshot.Partitions[plan.Key]
	if !exists || !sameAssignment(current, plan.OriginalAssignment) {
		return resetToPlanned(ErrStaleAssignment)
	}

	var commonHighWatermark uint64
	var commonEpoch uint64
	for i, brokerID := range plan.TargetReplicas {
		progress, inspectErr := provider.InspectReplica(plan.Key.Topic, uint32(plan.Key.Partition), brokerID)
		if inspectErr != nil {
			return resetToPlanned(fmt.Errorf("inspect target %q: %w", brokerID, inspectErr))
		}
		if progress.BrokerID != brokerID || progress.SourceLeader != plan.SourceLeader {
			return resetToPlanned(fmt.Errorf("target %q: %w", brokerID, ErrInvalidReplicaProgress))
		}
		if readinessErr := ValidateReplicaReadiness(progress); readinessErr != nil {
			return resetToPlanned(fmt.Errorf("target %q: %w", brokerID, readinessErr))
		}
		if i == 0 {
			commonHighWatermark = progress.HighWatermark
			commonEpoch = progress.LeaderEpoch
		} else if progress.HighWatermark != commonHighWatermark || progress.LeaderEpoch != commonEpoch {
			return resetToPlanned(fmt.Errorf("target %q reports different replication watermark/epoch: %w", brokerID, ErrInvalidReplicaProgress))
		}
	}

	newLeader := plan.SourceLeader
	if !slices.Contains(plan.TargetReplicas, newLeader) {
		newLeader = plan.TargetReplicas[0]
	}
	if current.LeaderEpoch == ^uint64(0) {
		return resetToPlanned(ErrInvalidConfig)
	}
	assignment := controller.PartitionAssignment{
		Key:         plan.Key,
		Replicas:    append([]string(nil), plan.TargetReplicas...),
		Leader:      newLeader,
		LeaderEpoch: current.LeaderEpoch + 1,
	}
	if err := c.manager.ControllerCluster().Commit(controller.Command{
		Type:       controller.CommandAssignPartition,
		Assignment: assignment,
	}); err != nil {
		// Phase 15 intentionally leaves a failed no-quorum proposal in the
		// Raft log as an uncommitted suffix. A higher-term response can also
		// arrive after the proposal was appended. Do not blindly retry either.
		if errors.Is(err, controller.ErrNoQuorum) || errors.Is(err, controller.ErrNotLeader) {
			return markUncertain(err)
		}
		return resetToPlanned(err)
	}

	c.mu.Lock()
	completed := c.plans[planID]
	completed.State = ReassignmentCompleted
	c.plans[planID] = completed
	delete(c.active, plan.Key)
	if err := c.persistLocked(); err != nil {
		c.mu.Unlock()
		return ReassignmentPlan{}, err
	}
	c.mu.Unlock()
	return clonePlan(completed), nil
}

func (c *ReassignmentCoordinator) Abort(planID string) error {
	if c == nil {
		return ErrPlanNotFound
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	plan, exists := c.plans[planID]
	if !exists {
		return ErrPlanNotFound
	}
	if plan.State != ReassignmentPlanned {
		return ErrPlanState
	}
	plan.State = ReassignmentAborted
	c.plans[planID] = plan
	delete(c.active, plan.Key)
	return c.persistLocked()
}

func (c *ReassignmentCoordinator) Plan(planID string) (ReassignmentPlan, error) {
	if c == nil {
		return ReassignmentPlan{}, ErrPlanNotFound
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	plan, exists := c.plans[planID]
	if !exists {
		return ReassignmentPlan{}, ErrPlanNotFound
	}
	return clonePlan(plan), nil
}

func clonePlan(plan ReassignmentPlan) ReassignmentPlan {
	plan.TargetReplicas = append([]string(nil), plan.TargetReplicas...)
	plan.OriginalAssignment = cloneAssignment(plan.OriginalAssignment)
	return plan
}

func cloneAssignment(assignment controller.PartitionAssignment) controller.PartitionAssignment {
	assignment.Replicas = append([]string(nil), assignment.Replicas...)
	assignment.ISR = append([]string(nil), assignment.ISR...)
	return assignment
}

func sameAssignment(a, b controller.PartitionAssignment) bool {
	return a.Key == b.Key && a.Leader == b.Leader && a.LeaderEpoch == b.LeaderEpoch && slices.Equal(a.Replicas, b.Replicas) && slices.Equal(a.ISR, b.ISR)
}

type reassignmentStoreState struct {
	Version  uint16                      `json:"version"`
	NextPlan uint64                      `json:"next_plan"`
	Plans    map[string]ReassignmentPlan `json:"plans"`
}

type reassignmentStore struct{ path string }

func openReassignmentStore(directory string) (*reassignmentStore, reassignmentStoreState, error) {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, reassignmentStoreState{}, err
	}
	store := &reassignmentStore{path: filepath.Join(directory, "reassignments.json")}
	data, err := os.ReadFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return store, reassignmentStoreState{Version: 1, Plans: make(map[string]ReassignmentPlan)}, nil
	}
	if err != nil {
		return nil, reassignmentStoreState{}, err
	}
	var state reassignmentStoreState
	if err := json.Unmarshal(data, &state); err != nil || state.Version != 1 || state.Plans == nil {
		return nil, reassignmentStoreState{}, fmt.Errorf("decode reassignment state: %w", ErrInvalidConfig)
	}
	return store, state, nil
}

func (c *ReassignmentCoordinator) persistLocked() error {
	if c.store == nil {
		return nil
	}
	state := reassignmentStoreState{Version: 1, NextPlan: c.nextPlan, Plans: make(map[string]ReassignmentPlan, len(c.plans))}
	for id, plan := range c.plans {
		state.Plans[id] = clonePlan(plan)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(c.store.path), ".reassignments-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	cleanup := func() { _ = temp.Close(); _ = os.Remove(tempPath) }
	if err := temp.Chmod(0600); err != nil {
		cleanup()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := temp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, c.store.path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	directory, err := os.Open(filepath.Dir(c.store.path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
