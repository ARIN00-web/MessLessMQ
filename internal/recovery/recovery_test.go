package recovery

import (
	"errors"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

func TestRecoverReplicaMarksAvailableCatchesUpAndVerifies(t *testing.T) {
	available, caughtUp, fenced := false, false, false
	task := ReplicaRecoveryTask{
		BrokerID: "broker-b", Topic: "orders", PartitionID: 1,
		MarkAvailable: func() error { available = true; return nil },
		CatchUp:       func() error { caughtUp = true; return nil },
		Inspect: func() (ReplicaReadiness, error) {
			return ready("broker-b", "broker-a", 3, 92), nil
		},
		MarkUnavailable: func() error { fenced = true; return nil },
	}
	result, err := RecoverReplica(task)
	if err != nil {
		t.Fatal(err)
	}
	if !available || !caughtUp || fenced || result.MatchOffset != 92 || !result.InSync {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRecoverReplicaRefencesOnCatchupFailure(t *testing.T) {
	fenced := false
	expected := errors.New("disk is behind retention")
	task := ReplicaRecoveryTask{
		BrokerID: "broker-b", Topic: "orders",
		MarkAvailable:   func() error { return nil },
		CatchUp:         func() error { return expected },
		Inspect:         func() (ReplicaReadiness, error) { t.Fatal("inspect should not run"); return ReplicaReadiness{}, nil },
		MarkUnavailable: func() error { fenced = true; return nil },
	}
	if _, err := RecoverReplica(task); !errors.Is(err, expected) {
		t.Fatalf("expected catch-up error, got %v", err)
	}
	if !fenced {
		t.Fatal("failed recovery did not fence replica")
	}
}

func TestRecoverBrokerReturnsPartialReport(t *testing.T) {
	good := func(topic string) ReplicaRecoveryTask {
		return ReplicaRecoveryTask{
			BrokerID: "broker-b", Topic: topic,
			MarkAvailable: func() error { return nil },
			CatchUp: func() error {
				if topic == "payments" {
					return errors.New("catch-up failed")
				}
				return nil
			},
			Inspect:         func() (ReplicaReadiness, error) { return ready("broker-b", "broker-a", 1, 20), nil },
			MarkUnavailable: func() error { return nil },
		}
	}
	result, err := RecoverBroker("broker-b", []ReplicaRecoveryTask{good("orders"), good("payments")})
	if !errors.Is(err, ErrBrokerRecoveryIncomplete) || len(result.Recovered) != 1 || len(result.Failed) != 1 {
		t.Fatalf("unexpected broker recovery result: %+v, %v", result, err)
	}
}

func TestRecoverConsumerGroupRebalancesAndValidatesUniqueOwnership(t *testing.T) {
	called := false
	result, err := RecoverConsumerGroup("billing", func(groupID string) (ConsumerGroupSnapshot, error) {
		called = true
		if groupID != "billing" {
			t.Fatalf("groupID = %q", groupID)
		}
		return ConsumerGroupSnapshot{
			GroupID: groupID, Generation: 9, Stable: true,
			Members: []GroupMemberSnapshot{
				{MemberID: "member-a", Assignments: []GroupPartition{{Topic: "orders", PartitionID: 0}}},
				{MemberID: "member-b", Assignments: []GroupPartition{{Topic: "orders", PartitionID: 1}}},
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called || result.Generation != 9 || result.Members != 2 || result.Partitions != 2 {
		t.Fatalf("unexpected group result: %+v", result)
	}
}

func TestRecoverConsumerGroupRejectsDuplicateOwners(t *testing.T) {
	_, err := RecoverConsumerGroup("billing", func(string) (ConsumerGroupSnapshot, error) {
		return ConsumerGroupSnapshot{
			GroupID: "billing", Generation: 1, Stable: true,
			Members: []GroupMemberSnapshot{
				{MemberID: "a", Assignments: []GroupPartition{{Topic: "orders", PartitionID: 0}}},
				{MemberID: "b", Assignments: []GroupPartition{{Topic: "orders", PartitionID: 0}}},
			},
		}, nil
	})
	if !errors.Is(err, ErrDuplicatePartitionOwner) {
		t.Fatalf("expected duplicate owner error, got %v", err)
	}
}

func TestRecoverControllerCatchesUpReturningFollower(t *testing.T) {
	cluster, err := controller.NewCluster("controller-a", "controller-b", "controller-c")
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Campaign("controller-a"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Commit(controller.Command{Type: controller.CommandRegisterBroker, Broker: controller.Broker{ID: "broker-a", Address: "127.0.0.1:9001"}}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.SetAvailable("controller-c", false); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Commit(controller.Command{Type: controller.CommandRegisterBroker, Broker: controller.Broker{ID: "broker-b", Address: "127.0.0.1:9002"}}); err != nil {
		t.Fatal(err)
	}
	if err := RecoverController(cluster, "controller-c"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := cluster.Metadata("controller-c")
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := snapshot.Brokers["broker-b"]; !exists {
		t.Fatal("recovered controller did not apply latest committed broker")
	}
}

func TestElectAndRecoverControllersAfterLeaderFailure(t *testing.T) {
	cluster, err := controller.NewCluster("controller-a", "controller-b", "controller-c")
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Campaign("controller-a"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.SetAvailable("controller-a", false); err != nil {
		t.Fatal(err)
	}
	if err := ElectAndRecoverControllers(cluster, "controller-b", []string{"controller-a", "controller-c"}); err != nil {
		t.Fatal(err)
	}
	if cluster.CurrentLeader() != "controller-b" {
		t.Fatalf("leader = %q", cluster.CurrentLeader())
	}
	if err := ValidateControllerStatuses(cluster.CurrentLeader(), cluster.Statuses()); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverReplicaFencesWhenInspectionReportsLag(t *testing.T) {
	fenced := false
	task := ReplicaRecoveryTask{
		BrokerID: "broker-b", Topic: "orders",
		MarkAvailable: func() error { return nil },
		CatchUp:       func() error { return nil },
		Inspect: func() (ReplicaReadiness, error) {
			progress := ready("broker-b", "broker-a", 2, 40)
			progress.MatchOffset = 39
			return progress, nil
		},
		MarkUnavailable: func() error { fenced = true; return nil },
	}
	if _, err := RecoverReplica(task); !errors.Is(err, ErrReplicaNotReady) {
		t.Fatalf("error = %v", err)
	}
	if !fenced {
		t.Fatal("replica was not fenced after failed verification")
	}
}

func TestRecoverBrokerRejectsTaskForAnotherBroker(t *testing.T) {
	task := ReplicaRecoveryTask{BrokerID: "broker-a", Topic: "orders"}
	if _, err := RecoverBroker("broker-b", []ReplicaRecoveryTask{task}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected config error, got %v", err)
	}
}

func TestRecoverControllerRequiresLeaderAfterReenable(t *testing.T) {
	cluster, err := controller.NewCluster("a", "b", "c")
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.SetAvailable("b", false); err != nil {
		t.Fatal(err)
	}
	if err := RecoverController(cluster, "b"); !errors.Is(err, ErrNoControllerLeader) {
		t.Fatalf("expected no leader, got %v", err)
	}
}

func TestRecoverControllerRejectsUnknownNode(t *testing.T) {
	cluster, err := controller.NewCluster("a", "b", "c")
	if err != nil {
		t.Fatal(err)
	}
	if err := RecoverController(cluster, "missing"); !errors.Is(err, controller.ErrUnknownNode) {
		t.Fatalf("expected unknown node, got %v", err)
	}
}

func TestElectAndRecoverControllersSyncsCandidateWhenLeaderAlreadyExists(t *testing.T) {
	cluster, err := controller.NewCluster("controller-a", "controller-b", "controller-c")
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Campaign("controller-a"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Commit(controller.Command{Type: controller.CommandRegisterBroker, Broker: controller.Broker{ID: "broker-a", Address: "127.0.0.1:9001"}}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.SetAvailable("controller-c", false); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Commit(controller.Command{Type: controller.CommandRegisterBroker, Broker: controller.Broker{ID: "broker-b", Address: "127.0.0.1:9002"}}); err != nil {
		t.Fatal(err)
	}
	if err := ElectAndRecoverControllers(cluster, "controller-c", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := cluster.Metadata("controller-c")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Brokers["broker-b"]; !ok {
		t.Fatal("candidate was not synchronized from current leader")
	}
}
