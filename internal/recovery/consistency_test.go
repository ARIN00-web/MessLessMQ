package recovery

import (
	"errors"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

func TestValidateMetadataSnapshot(t *testing.T) {
	snapshot := controller.MetadataSnapshot{
		Brokers: map[string]controller.Broker{
			"a": {ID: "a", Address: "a:1"},
			"b": {ID: "b", Address: "b:1"},
		},
		Partitions: map[controller.PartitionKey]controller.PartitionAssignment{
			{Topic: "orders", Partition: 0}: {
				Key:      controller.PartitionKey{Topic: "orders", Partition: 0},
				Replicas: []string{"a", "b"}, Leader: "a", LeaderEpoch: 1,
			},
		},
	}
	if err := ValidateMetadataSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Partitions[controller.PartitionKey{Topic: "orders", Partition: 0}] = controller.PartitionAssignment{
		Key: controller.PartitionKey{Topic: "orders", Partition: 0}, Replicas: []string{"a", "missing"}, Leader: "a", LeaderEpoch: 1,
	}
	if err := ValidateMetadataSnapshot(snapshot); !errors.Is(err, ErrControllerStateInconsistent) {
		t.Fatalf("expected inconsistent metadata, got %v", err)
	}
}

func TestValidateControllerStatuses(t *testing.T) {
	statuses := map[string]controller.NodeStatus{
		"a": {ID: "a", Role: controller.RoleLeader, LastLogIndex: 3, CommitIndex: 2, AppliedIndex: 2},
		"b": {ID: "b", Role: controller.RoleFollower, LastLogIndex: 3, CommitIndex: 2, AppliedIndex: 2},
	}
	if err := ValidateControllerStatuses("a", statuses); err != nil {
		t.Fatal(err)
	}
	statuses["b"] = controller.NodeStatus{ID: "b", Role: controller.RoleLeader, LastLogIndex: 3, CommitIndex: 2, AppliedIndex: 2}
	if err := ValidateControllerStatuses("a", statuses); !errors.Is(err, ErrControllerStateInconsistent) {
		t.Fatalf("expected multiple leader error, got %v", err)
	}
}

func TestValidateReplicaReadiness(t *testing.T) {
	if err := ValidateReplicaReadiness(ready("b", "a", 1, 10)); err != nil {
		t.Fatal(err)
	}
	behind := ready("b", "a", 1, 10)
	behind.MatchOffset = 9
	if err := ValidateReplicaReadiness(behind); !errors.Is(err, ErrReplicaNotReady) {
		t.Fatalf("expected not-ready error, got %v", err)
	}
}

func TestValidateGroupSnapshotRejectsDuplicateOwnership(t *testing.T) {
	snapshot := ConsumerGroupSnapshot{
		GroupID: "g", Generation: 1, Stable: true,
		Members: []GroupMemberSnapshot{
			{MemberID: "a", Assignments: []GroupPartition{{Topic: "orders", PartitionID: 0}}},
			{MemberID: "b", Assignments: []GroupPartition{{Topic: "orders", PartitionID: 0}}},
		},
	}
	if err := ValidateGroupSnapshot(snapshot); !errors.Is(err, ErrDuplicatePartitionOwner) {
		t.Fatalf("expected duplicate owner error, got %v", err)
	}
}
