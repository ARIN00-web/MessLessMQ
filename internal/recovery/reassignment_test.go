package recovery

import (
	"errors"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/cluster"
	"github.com/ARIN00-web/messlessmq/internal/controller"
)

type readinessMap map[string]ReplicaReadiness

func (m readinessMap) InspectReplica(topic string, partition uint32, brokerID string) (ReplicaReadiness, error) {
	progress, ok := m[brokerID]
	if !ok {
		return ReplicaReadiness{}, ErrInvalidReplicaProgress
	}
	return progress, nil
}

func newClusterManager(t *testing.T) *cluster.Manager {
	t.Helper()
	manager, err := cluster.NewManager(cluster.Config{
		ControllerIDs:   []string{"controller-a", "controller-b", "controller-c"},
		BootstrapLeader: "controller-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"broker-a", "broker-b", "broker-c"} {
		if err := manager.RegisterBroker(controller.Broker{ID: id, Address: string(rune('1'+i)) + ":9001"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := manager.PlacePartition("orders", 0, 2); err != nil {
		t.Fatal(err)
	}
	return manager
}

func ready(brokerID, source string, epoch, end uint64) ReplicaReadiness {
	return ReplicaReadiness{
		BrokerID: brokerID, SourceLeader: source, LeaderEpoch: epoch,
		MatchOffset: end, HighWatermark: end, Available: true, InSync: true,
	}
}

func TestReassignmentRequiresEveryTargetCaughtUpBeforeMetadataCommit(t *testing.T) {
	manager := newClusterManager(t)
	coordinator, err := NewReassignmentCoordinator(manager)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := coordinator.Begin("orders", 0, []string{"broker-b", "broker-c"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.SourceLeader != "broker-a" || plan.State != ReassignmentPlanned {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	provider := readinessMap{
		"broker-b": ready("broker-b", "broker-a", 4, 128),
		"broker-c": ready("broker-c", "broker-a", 4, 128),
	}
	completed, err := coordinator.Complete(plan.ID, provider)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != ReassignmentCompleted {
		t.Fatalf("state = %s", completed.State)
	}
	snapshot, err := manager.Metadata(manager.ControllerLeader())
	if err != nil {
		t.Fatal(err)
	}
	assignment := snapshot.Partitions[controller.PartitionKey{Topic: "orders", Partition: 0}]
	if assignment.Leader != "broker-b" || assignment.LeaderEpoch != 2 || len(assignment.Replicas) != 2 || assignment.Replicas[0] != "broker-b" || assignment.Replicas[1] != "broker-c" {
		t.Fatalf("unexpected committed assignment: %+v", assignment)
	}
	if err := ValidateMetadataSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
}

func TestReassignmentDoesNotCommitWhenTargetIsBehind(t *testing.T) {
	manager := newClusterManager(t)
	coordinator, _ := NewReassignmentCoordinator(manager)
	plan, err := coordinator.Begin("orders", 0, []string{"broker-b", "broker-c"})
	if err != nil {
		t.Fatal(err)
	}
	behind := ready("broker-c", "broker-a", 4, 128)
	behind.MatchOffset = 127
	provider := readinessMap{
		"broker-b": ready("broker-b", "broker-a", 4, 128),
		"broker-c": behind,
	}
	if _, err := coordinator.Complete(plan.ID, provider); !errors.Is(err, ErrReplicaNotReady) {
		t.Fatalf("expected replica-not-ready, got %v", err)
	}
	state, err := coordinator.Plan(plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != ReassignmentPlanned {
		t.Fatalf("failed readiness should leave plan retryable, got %s", state.State)
	}
	snapshot, err := manager.Metadata(manager.ControllerLeader())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Partitions[plan.Key].Leader != "broker-a" {
		t.Fatal("metadata changed before all targets caught up")
	}
}

func TestReassignmentRejectsStaleAssignment(t *testing.T) {
	manager := newClusterManager(t)
	coordinator, _ := NewReassignmentCoordinator(manager)
	plan, err := coordinator.Begin("orders", 0, []string{"broker-b", "broker-c"})
	if err != nil {
		t.Fatal(err)
	}
	current, _ := manager.ResolveRoute("orders", 0)
	changed := current.Assignment
	changed.LeaderEpoch++
	if err := manager.ControllerCluster().Commit(controller.Command{Type: controller.CommandAssignPartition, Assignment: changed}); err != nil {
		t.Fatal(err)
	}
	provider := readinessMap{
		"broker-b": ready("broker-b", "broker-a", 4, 128),
		"broker-c": ready("broker-c", "broker-a", 4, 128),
	}
	if _, err := coordinator.Complete(plan.ID, provider); !errors.Is(err, ErrStaleAssignment) {
		t.Fatalf("expected stale assignment, got %v", err)
	}
}

func TestReassignmentRejectsConcurrentPlanAndSupportsAbort(t *testing.T) {
	manager := newClusterManager(t)
	coordinator, _ := NewReassignmentCoordinator(manager)
	plan, err := coordinator.Begin("orders", 0, []string{"broker-b", "broker-c"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Begin("orders", 0, []string{"broker-a", "broker-c"}); !errors.Is(err, ErrPlanExists) {
		t.Fatalf("expected active plan conflict, got %v", err)
	}
	if err := coordinator.Abort(plan.ID); err != nil {
		t.Fatal(err)
	}
	state, _ := coordinator.Plan(plan.ID)
	if state.State != ReassignmentAborted {
		t.Fatalf("state = %s", state.State)
	}
}

func TestReassignmentRejectsMismatchedProgress(t *testing.T) {
	manager := newClusterManager(t)
	coordinator, _ := NewReassignmentCoordinator(manager)
	plan, err := coordinator.Begin("orders", 0, []string{"broker-b", "broker-c"})
	if err != nil {
		t.Fatal(err)
	}
	provider := readinessMap{
		"broker-b": ready("broker-b", "broker-a", 4, 128),
		"broker-c": ready("broker-c", "broker-x", 4, 128),
	}
	if _, err := coordinator.Complete(plan.ID, provider); !errors.Is(err, ErrInvalidReplicaProgress) {
		t.Fatalf("expected invalid evidence, got %v", err)
	}
}

func TestReassignmentCommitWithoutQuorumBecomesUncertain(t *testing.T) {
	manager := newClusterManager(t)
	controllers := manager.ControllerCluster()
	if err := controllers.SetLink("controller-a", "controller-b", false); err != nil {
		t.Fatal(err)
	}
	if err := controllers.SetLink("controller-a", "controller-c", false); err != nil {
		t.Fatal(err)
	}
	coordinator, _ := NewReassignmentCoordinator(manager)
	plan, err := coordinator.Begin("orders", 0, []string{"broker-b", "broker-c"})
	if err != nil {
		t.Fatal(err)
	}
	provider := readinessMap{
		"broker-b": ready("broker-b", "broker-a", 4, 128),
		"broker-c": ready("broker-c", "broker-a", 4, 128),
	}
	if _, err := coordinator.Complete(plan.ID, provider); !errors.Is(err, controller.ErrNoQuorum) {
		t.Fatalf("expected no-quorum error, got %v", err)
	}
	stored, err := coordinator.Plan(plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != ReassignmentCommitUncertain {
		t.Fatalf("state = %s", stored.State)
	}
	if err := coordinator.Abort(plan.ID); !errors.Is(err, ErrPlanState) {
		t.Fatalf("uncertain plan should not be abortable, got %v", err)
	}
}

func TestBeginRejectsUnknownBrokerAndInvalidTargetSet(t *testing.T) {
	manager := newClusterManager(t)
	coordinator, _ := NewReassignmentCoordinator(manager)
	if _, err := coordinator.Begin("orders", 0, []string{"broker-b", "missing"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected unknown broker error, got %v", err)
	}
	if _, err := coordinator.Begin("orders", 0, []string{"broker-b", "broker-b"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected duplicate target error, got %v", err)
	}
}

func TestReassignmentRejectsMissingPlanAndInvalidState(t *testing.T) {
	manager := newClusterManager(t)
	coordinator, _ := NewReassignmentCoordinator(manager)
	if _, err := coordinator.Complete("missing", readinessMap{}); !errors.Is(err, ErrPlanNotFound) {
		t.Fatalf("expected plan-not-found, got %v", err)
	}
	plan, err := coordinator.Begin("orders", 0, []string{"broker-b", "broker-c"})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Abort(plan.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Complete(plan.ID, readinessMap{}); !errors.Is(err, ErrPlanState) {
		t.Fatalf("expected invalid plan state, got %v", err)
	}
}

func TestReassignmentPlansPersistAcrossCoordinatorRestart(t *testing.T) {
	manager := newClusterManager(t)
	directory := t.TempDir()
	coordinator, err := OpenReassignmentCoordinator(manager, directory)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := coordinator.Begin("orders", 0, []string{"broker-b", "broker-c"})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenReassignmentCoordinator(manager, directory)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := restarted.Plan(plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if restored.State != ReassignmentPlanned || restored.Key != plan.Key || restored.SourceLeader != "broker-a" {
		t.Fatalf("restored plan = %+v", restored)
	}
	if _, err := restarted.Begin("orders", 0, []string{"broker-a", "broker-c"}); !errors.Is(err, ErrPlanExists) {
		t.Fatalf("restored active plan did not prevent conflict: %v", err)
	}
}
