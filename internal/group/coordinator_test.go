package group

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func setupRegistry(t *testing.T, specs map[string]uint32) (*topic.Registry, string) {
	t.Helper()
	registry := topic.NewRegistry()
	root := t.TempDir()

	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)

	opened := make([]*topic.Topic, 0, len(names))
	for _, name := range names {
		created, err := registry.CreateWithPartitionCount(name, specs[name])
		if err != nil {
			t.Fatalf("CreateWithPartitionCount(%q): %v", name, err)
		}
		if err := created.OpenLogs(root, 1024); err != nil {
			t.Fatalf("OpenLogs(%q): %v", name, err)
		}
		opened = append(opened, created)
	}

	t.Cleanup(func() {
		for _, created := range opened {
			if err := created.CloseLogs(); err != nil {
				t.Errorf("CloseLogs(%q): %v", created.Name(), err)
			}
		}
	})
	return registry, root
}

func newTestCoordinator(t *testing.T, specs map[string]uint32) *Coordinator {
	t.Helper()
	registry, _ := setupRegistry(t, specs)
	coordinator, err := NewCoordinator(registry)
	if err != nil {
		t.Fatalf("NewCoordinator(): %v", err)
	}
	return coordinator
}

func assignmentSet(assignments []TopicPartition) map[TopicPartition]bool {
	result := make(map[TopicPartition]bool, len(assignments))
	for _, assignment := range assignments {
		result[assignment] = true
	}
	return result
}

func TestNewCoordinatorRejectsNilRegistry(t *testing.T) {
	_, err := NewCoordinator(nil)
	if !errors.Is(err, ErrNilRegistry) {
		t.Fatalf("error = %v, want ErrNilRegistry", err)
	}
}

func TestJoinGroupAssignsAllPartitionsToFirstMember(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 3})

	result, err := coordinator.JoinGroup("orders-workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinGroup(): %v", err)
	}
	if result.Generation != 1 {
		t.Fatalf("generation = %d, want 1", result.Generation)
	}
	if len(result.Assignments) != 3 {
		t.Fatalf("assignment count = %d, want 3", len(result.Assignments))
	}
	for id := uint32(0); id < 3; id++ {
		if !assignmentSet(result.Assignments)[TopicPartition{Topic: "orders", PartitionID: id}] {
			t.Errorf("member missing orders partition %d", id)
		}
	}
}

func TestJoinGroupBalancesPartitionsDeterministically(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 4})

	if _, err := coordinator.JoinGroup("workers", "member-b", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	joined, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	if joined.Generation != 2 {
		t.Fatalf("generation = %d, want 2", joined.Generation)
	}

	description, err := coordinator.DescribeGroup("workers")
	if err != nil {
		t.Fatal(err)
	}
	if description.State != StateStable {
		t.Fatalf("state = %s, want Stable", description.State)
	}
	if len(description.Members) != 2 {
		t.Fatalf("member count = %d, want 2", len(description.Members))
	}

	// Member IDs are sorted before round-robin assignment. member-a receives
	// partitions 0 and 2; member-b receives partitions 1 and 3.
	if len(description.Members[0].Assignments) != 2 || description.Members[0].MemberID != "member-a" {
		t.Fatalf("unexpected member-a assignment: %+v", description.Members[0])
	}
	if len(description.Members[1].Assignments) != 2 || description.Members[1].MemberID != "member-b" {
		t.Fatalf("unexpected member-b assignment: %+v", description.Members[1])
	}
}

func TestGroupPartitionOwnershipIsUnique(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 7})
	members := []string{"a", "b", "c"}
	for _, member := range members {
		if _, err := coordinator.JoinGroup("workers", member, []string{"orders"}); err != nil {
			t.Fatal(err)
		}
	}

	description, err := coordinator.DescribeGroup("workers")
	if err != nil {
		t.Fatal(err)
	}

	owners := make(map[TopicPartition]string)
	for _, member := range description.Members {
		for _, assignment := range member.Assignments {
			if previous, exists := owners[assignment]; exists {
				t.Fatalf("partition %+v assigned to both %q and %q", assignment, previous, member.MemberID)
			}
			owners[assignment] = member.MemberID
		}
	}
	if len(owners) != 7 {
		t.Fatalf("unique owned partitions = %d, want 7", len(owners))
	}
}

func TestGroupSupportsTopicSpecificSubscriptions(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 3, "payments": 2})

	if _, err := coordinator.JoinGroup("services", "orders-worker", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.JoinGroup("services", "payment-worker", []string{"payments"}); err != nil {
		t.Fatal(err)
	}

	description, err := coordinator.DescribeGroup("services")
	if err != nil {
		t.Fatal(err)
	}

	for _, member := range description.Members {
		for _, assignment := range member.Assignments {
			if member.MemberID == "orders-worker" && assignment.Topic != "orders" {
				t.Fatalf("orders worker received %+v", assignment)
			}
			if member.MemberID == "payment-worker" && assignment.Topic != "payments" {
				t.Fatalf("payment worker received %+v", assignment)
			}
		}
	}
	if len(description.Members[0].Assignments) != 3 || len(description.Members[1].Assignments) != 2 {
		t.Fatalf("unexpected topic-specific assignments: %+v", description.Members)
	}
}

func TestMemberCanSubscribeToMultipleTopics(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 2, "payments": 1})

	joined, err := coordinator.JoinGroup("all-events", "worker-a", []string{"payments", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if len(joined.Assignments) != 3 {
		t.Fatalf("assignment count = %d, want 3", len(joined.Assignments))
	}
}

func TestDifferentGroupsCanOwnSamePartitionIndependently(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 1})

	first, err := coordinator.JoinGroup("analytics", "analytics-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := coordinator.JoinGroup("notifications", "notifications-a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	want := TopicPartition{Topic: "orders", PartitionID: 0}
	if len(first.Assignments) != 1 || first.Assignments[0] != want {
		t.Fatalf("first group assignments = %+v", first.Assignments)
	}
	if len(second.Assignments) != 1 || second.Assignments[0] != want {
		t.Fatalf("second group assignments = %+v", second.Assignments)
	}
}

func TestJoinGroupRejectsInvalidMembership(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 1})

	tests := []struct {
		name     string
		groupID  string
		memberID string
		topics   []string
		want     error
	}{
		{"empty group ID", "", "member-a", []string{"orders"}, ErrInvalidGroupID},
		{"invalid group ID", "bad/group", "member-a", []string{"orders"}, ErrInvalidGroupID},
		{"empty member ID", "workers", "", []string{"orders"}, ErrInvalidMemberID},
		{"empty subscription", "workers", "member-a", nil, ErrInvalidSubscription},
		{"missing topic", "workers", "member-a", []string{"missing"}, ErrSubscribedTopicMissing},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := coordinator.JoinGroup(tt.groupID, tt.memberID, tt.topics)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestJoinGroupRejectsDuplicateMember(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 1})
	if _, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	_, err := coordinator.JoinGroup("workers", "member-a", []string{"orders"})
	if !errors.Is(err, ErrMemberAlreadyExists) {
		t.Fatalf("error = %v, want ErrMemberAlreadyExists", err)
	}
}

func TestLeaveGroupReassignsPartitions(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 4})
	if _, err := coordinator.JoinGroup("workers", "a", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.JoinGroup("workers", "b", []string{"orders"}); err != nil {
		t.Fatal(err)
	}

	generation, err := coordinator.LeaveGroup("workers", "b")
	if err != nil {
		t.Fatal(err)
	}

	description, err := coordinator.DescribeGroup("workers")
	if err != nil {
		t.Fatal(err)
	}
	if generation != 3 || description.Generation != 3 {
		t.Fatalf("generation = %d, want 3", generation)
	}
	if len(description.Members) != 1 || len(description.Members[0].Assignments) != 4 {
		t.Fatalf("remaining member did not receive all partitions: %+v", description.Members)
	}
}

func TestLeaveLastMemberMakesGroupEmpty(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 1})
	if _, err := coordinator.JoinGroup("workers", "a", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.LeaveGroup("workers", "a"); err != nil {
		t.Fatal(err)
	}

	description, err := coordinator.DescribeGroup("workers")
	if err != nil {
		t.Fatal(err)
	}
	if description.State != StateEmpty || len(description.Members) != 0 {
		t.Fatalf("description = %+v, want empty group", description)
	}
}

func TestHeartbeatRejectsStaleGeneration(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 2})
	first, err := coordinator.JoinGroup("workers", "a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.JoinGroup("workers", "b", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Heartbeat("workers", "a", first.Generation); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("Heartbeat() error = %v, want ErrStaleGeneration", err)
	}
}

func TestHeartbeatRefreshesMemberLiveness(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 1})
	joined, err := coordinator.JoinGroup("workers", "a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Heartbeat("workers", "a", joined.Generation); err != nil {
		t.Fatalf("Heartbeat() error: %v", err)
	}
}

func TestExpireMembersRemovesTimedOutMemberAndRebalances(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 3})
	if _, err := coordinator.JoinGroup("workers", "a", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.JoinGroup("workers", "b", []string{"orders"}); err != nil {
		t.Fatal(err)
	}

	expired, err := coordinator.ExpireMembers(time.Now().Add(time.Hour), time.Minute)
	if err != nil {
		t.Fatalf("ExpireMembers() error: %v", err)
	}
	if len(expired) != 2 {
		t.Fatalf("expired count = %d, want 2", len(expired))
	}

	description, err := coordinator.DescribeGroup("workers")
	if err != nil {
		t.Fatal(err)
	}
	if description.State != StateEmpty || len(description.Members) != 0 {
		t.Fatalf("description = %+v, want empty group", description)
	}
}

func TestExpireMembersRejectsInvalidTimeout(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 1})
	_, err := coordinator.ExpireMembers(time.Now(), 0)
	if !errors.Is(err, ErrInvalidSessionTimeout) {
		t.Fatalf("error = %v, want ErrInvalidSessionTimeout", err)
	}
}

func TestGroupFetchRequiresCurrentOwnershipAndGeneration(t *testing.T) {
	registry, _ := setupRegistry(t, map[string]uint32{"orders": 2})
	orders, err := registry.Get("orders")
	if err != nil {
		t.Fatal(err)
	}
	partition0, _ := orders.Partition(0)
	if _, err := partition0.Append([]byte("event-0")); err != nil {
		t.Fatal(err)
	}
	if _, err := partition0.Append([]byte("event-1")); err != nil {
		t.Fatal(err)
	}

	coordinator, err := NewCoordinator(registry)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := coordinator.JoinGroup("workers", "a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := coordinator.Fetch("workers", "a", joined.Generation, "orders", 0, 1)
	if err != nil {
		t.Fatalf("Fetch() error: %v", err)
	}
	if len(result.Records) != 1 || string(result.Records[0].Value) != "event-0" {
		t.Fatalf("unexpected fetch result: %+v", result)
	}

	if _, err := coordinator.JoinGroup("workers", "b", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.Fetch("workers", "a", joined.Generation, "orders", 0, 1)
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale fetch error = %v, want ErrStaleGeneration", err)
	}
}

func TestGroupFetchRejectsPartitionOwnedByAnotherMember(t *testing.T) {
	registry, _ := setupRegistry(t, map[string]uint32{"orders": 2})
	coordinator, err := NewCoordinator(registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.JoinGroup("workers", "a", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	joinedB, err := coordinator.JoinGroup("workers", "b", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = coordinator.Fetch("workers", "b", joinedB.Generation, "orders", 0, 1)
	if !errors.Is(err, ErrPartitionNotOwned) {
		t.Fatalf("Fetch() error = %v, want ErrPartitionNotOwned", err)
	}
}

func TestRebalancePreservesPositionsForRetainedOwnership(t *testing.T) {
	registry, _ := setupRegistry(t, map[string]uint32{"orders": 1})
	orders, _ := registry.Get("orders")
	partition, _ := orders.Partition(0)
	if _, err := partition.Append([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append([]byte("second")); err != nil {
		t.Fatal(err)
	}

	coordinator, _ := NewCoordinator(registry)
	joined, err := coordinator.JoinGroup("workers", "a", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Fetch("workers", "a", joined.Generation, "orders", 0, 1); err != nil {
		t.Fatal(err)
	}

	generation, err := coordinator.Rebalance("workers")
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Fetch("workers", "a", generation, "orders", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || string(result.Records[0].Value) != "second" {
		t.Fatalf("position was not preserved: %+v", result)
	}
}

func TestMemberWithNoAssignedPartitionsHasNoFetchAccess(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 1})
	if _, err := coordinator.JoinGroup("workers", "a", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	joined, err := coordinator.JoinGroup("workers", "b", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.Fetch("workers", "b", joined.Generation, "orders", 0, 1)
	if !errors.Is(err, ErrPartitionNotOwned) {
		t.Fatalf("Fetch() error = %v, want ErrPartitionNotOwned", err)
	}
}

func TestDescribeGroupReturnsSortedSnapshots(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 2, "payments": 1})
	if _, err := coordinator.JoinGroup("workers", "z-member", []string{"payments", "orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.JoinGroup("workers", "a-member", []string{"orders"}); err != nil {
		t.Fatal(err)
	}

	description, err := coordinator.DescribeGroup("workers")
	if err != nil {
		t.Fatal(err)
	}
	if description.Members[0].MemberID != "a-member" || description.Members[1].MemberID != "z-member" {
		t.Fatalf("members are not sorted: %+v", description.Members)
	}
	if len(description.Members[1].Subscriptions) != 2 || description.Members[1].Subscriptions[0] != "orders" {
		t.Fatalf("subscriptions are not normalized: %+v", description.Members[1].Subscriptions)
	}
}

func TestConcurrentJoinsProduceUniquePartitionOwnership(t *testing.T) {
	coordinator := newTestCoordinator(t, map[string]uint32{"orders": 12})
	const count = 8
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := coordinator.JoinGroup("workers", fmt.Sprintf("member-%02d", i), []string{"orders"})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("JoinGroup() error: %v", err)
	}

	description, err := coordinator.DescribeGroup("workers")
	if err != nil {
		t.Fatal(err)
	}
	owners := make(map[TopicPartition]bool)
	for _, member := range description.Members {
		for _, assignment := range member.Assignments {
			if owners[assignment] {
				t.Fatalf("partition %+v has more than one owner", assignment)
			}
			owners[assignment] = true
		}
	}
	if len(owners) != 12 {
		t.Fatalf("owned partition count = %d, want 12", len(owners))
	}
}
