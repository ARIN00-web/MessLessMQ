package replication

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/storage"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

type testReplica struct {
	topic     *topic.Topic
	partition *topic.Partition
	replica   *Replica
}

func openTestReplica(
	t *testing.T,
	id ReplicaID,
	topicName string,
	segmentSize int64,
	seed []Record,
) testReplica {
	t.Helper()
	registry := topic.NewRegistry()
	topicObject, err := registry.CreateWithPartitionCount(topicName, 1)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount() error: %v", err)
	}
	if err := topicObject.OpenLogs(t.TempDir(), segmentSize); err != nil {
		t.Fatalf("OpenLogs() error: %v", err)
	}
	t.Cleanup(func() {
		if err := topicObject.CloseLogs(); err != nil {
			t.Errorf("CloseLogs() error: %v", err)
		}
	})
	partition, err := topicObject.Partition(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range seed {
		offset, err := partition.AppendAtSync(record.Value, record.Timestamp)
		if err != nil {
			t.Fatalf("seed AppendAtSync() error: %v", err)
		}
		if offset != record.Offset {
			t.Fatalf("seed offset = %d, want %d", offset, record.Offset)
		}
	}
	replica, err := NewReplica(id, partition)
	if err != nil {
		t.Fatalf("NewReplica(%q) error: %v", id, err)
	}
	return testReplica{topic: topicObject, partition: partition, replica: replica}
}

func newTestManager(
	t *testing.T,
	count int,
	minISR int,
	seed []Record,
) (*Manager, map[ReplicaID]testReplica) {
	t.Helper()
	replicas := make([]*Replica, 0, count)
	local := make(map[ReplicaID]testReplica, count)
	for i := 1; i <= count; i++ {
		id := ReplicaID(fmt.Sprintf("broker-%d", i))
		r := openTestReplica(t, id, "orders", 1024*1024, seed)
		replicas = append(replicas, r.replica)
		local[id] = r
	}
	manager, err := NewManager(replicas, Config{
		LeaderID:                "broker-1",
		MinISR:                  minISR,
		ReplicationBatchRecords: 2,
		ReplicationBatchBytes:   1024,
	})
	if err != nil {
		t.Fatalf("NewManager() error: %v", err)
	}
	return manager, local
}

func TestNewManagerRejectsDivergentInitialReplica(t *testing.T) {
	leader := openTestReplica(t, "broker-1", "orders", 1024, []Record{{Offset: 0, Timestamp: 10, Value: []byte("leader")}})
	follower := openTestReplica(t, "broker-2", "orders", 1024, []Record{{Offset: 0, Timestamp: 10, Value: []byte("follower")}})

	_, err := NewManager([]*Replica{leader.replica, follower.replica}, Config{
		LeaderID: "broker-1",
		MinISR:   1,
	})
	if !errors.Is(err, ErrDivergentReplica) {
		t.Fatalf("NewManager() error = %v, want ErrDivergentReplica", err)
	}
}

func TestNewManagerRejectsReplicaWithDifferentLogEnd(t *testing.T) {
	leader := openTestReplica(t, "broker-1", "orders", 1024, []Record{{Offset: 0, Timestamp: 10, Value: []byte("one")}})
	follower := openTestReplica(t, "broker-2", "orders", 1024, nil)

	_, err := NewManager([]*Replica{leader.replica, follower.replica}, Config{
		LeaderID: "broker-1",
		MinISR:   1,
	})
	if !errors.Is(err, ErrReplicaNotCaughtUp) {
		t.Fatalf("NewManager() error = %v, want ErrReplicaNotCaughtUp", err)
	}
}

func TestNewManagerRejectsPartitionMismatch(t *testing.T) {
	leader := openTestReplica(t, "broker-1", "orders", 1024, nil)
	other := openTestReplica(t, "broker-2", "payments", 1024, nil)

	_, err := NewManager([]*Replica{leader.replica, other.replica}, Config{
		LeaderID: "broker-1",
		MinISR:   1,
	})
	if !errors.Is(err, ErrPartitionMismatch) {
		t.Fatalf("NewManager() error = %v, want ErrPartitionMismatch", err)
	}
}

func TestManagerAppendReplicatesAndAdvancesHighWatermark(t *testing.T) {
	manager, local := newTestManager(t, 3, 2, nil)

	result, err := manager.AppendAt([]byte("order-created"), 1000)
	if err != nil {
		t.Fatalf("AppendAt() error: %v", err)
	}
	if result.Offset != 0 {
		t.Fatalf("offset = %d, want 0", result.Offset)
	}
	if result.HighWatermark != 1 {
		t.Fatalf("high watermark = %d, want 1", result.HighWatermark)
	}
	if result.AcknowledgedReplicas != 3 {
		t.Fatalf("acknowledged replicas = %d, want 3", result.AcknowledgedReplicas)
	}
	if got := manager.HighWatermark(); got != 1 {
		t.Fatalf("HighWatermark() = %d, want 1", got)
	}
	if got := len(manager.InSyncReplicas()); got != 3 {
		t.Fatalf("ISR count = %d, want 3", got)
	}

	for id, replica := range local {
		got, timestamp, err := replica.partition.ReadWithTimestamp(0)
		if err != nil {
			t.Fatalf("replica %s ReadWithTimestamp() error: %v", id, err)
		}
		if string(got) != "order-created" || timestamp != 1000 {
			t.Fatalf("replica %s record = (%q,%d), want (order-created,1000)", id, got, timestamp)
		}
	}

	committed, err := manager.ReadCommitted(0, 10, 1024)
	if err != nil {
		t.Fatalf("ReadCommitted() error: %v", err)
	}
	if len(committed.Records) != 1 || committed.NextOffset != 1 {
		t.Fatalf("unexpected committed read: %+v", committed)
	}
}

func TestHighWatermarkHidesUnreplicatedLeaderTail(t *testing.T) {
	manager, local := newTestManager(t, 2, 1, nil)
	leader := local["broker-1"].partition

	if _, err := leader.AppendAtSync([]byte("not-replicated"), 123); err != nil {
		t.Fatal(err)
	}
	if got := manager.HighWatermark(); got != 0 {
		t.Fatalf("HighWatermark() = %d, want 0", got)
	}

	result, err := manager.ReadCommitted(0, 10, 1024)
	if err != nil {
		t.Fatalf("ReadCommitted() error: %v", err)
	}
	if len(result.Records) != 0 || result.NextOffset != 0 {
		t.Fatalf("unreplicated tail was exposed: %+v", result)
	}
	if _, err := manager.ReadCommitted(1, 10, 1024); !errors.Is(err, ErrHighWatermarkRange) {
		t.Fatalf("ReadCommitted(1) error = %v, want ErrHighWatermarkRange", err)
	}
}

func TestMinimumISRBlocksAppendBeforeLeaderWrite(t *testing.T) {
	manager, local := newTestManager(t, 3, 2, nil)
	if err := manager.MarkReplicaUnavailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaUnavailable("broker-3"); err != nil {
		t.Fatal(err)
	}

	result, err := manager.Append([]byte("should-not-append"))
	if !errors.Is(err, ErrInsufficientISR) {
		t.Fatalf("Append() error = %v, want ErrInsufficientISR", err)
	}
	if result.Offset != 0 || result.HighWatermark != 0 {
		t.Fatalf("unexpected result on rejected append: %+v", result)
	}
	next, err := local["broker-1"].partition.NextOffset()
	if err != nil {
		t.Fatal(err)
	}
	if next != 0 {
		t.Fatalf("leader next offset = %d, want 0", next)
	}
}

func TestFailedFollowerCanCatchUpAndRejoinISR(t *testing.T) {
	manager, local := newTestManager(t, 2, 1, nil)
	follower := local["broker-2"].partition

	if err := manager.MarkReplicaUnavailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if got := len(manager.InSyncReplicas()); got != 1 {
		t.Fatalf("ISR count after follower failure = %d, want 1", got)
	}

	if _, err := manager.AppendAt([]byte("during-outage"), 111); err != nil {
		t.Fatalf("leader append during follower outage: %v", err)
	}
	if err := manager.MarkReplicaAvailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if err := manager.CatchUpReplica("broker-2"); err != nil {
		t.Fatalf("CatchUpReplica() error: %v", err)
	}

	got, ts, err := follower.ReadWithTimestamp(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "during-outage" || ts != 111 {
		t.Fatalf("caught-up record = (%q,%d)", got, ts)
	}
	if got := len(manager.InSyncReplicas()); got != 2 {
		t.Fatalf("ISR count after catch-up = %d, want 2", got)
	}

	result, err := manager.AppendAt([]byte("after-rejoin"), 222)
	if err != nil {
		t.Fatalf("append after rejoin: %v", err)
	}
	if result.HighWatermark != 2 {
		t.Fatalf("high watermark = %d, want 2", result.HighWatermark)
	}
}

func TestFollowerWriteFailureLeavesUncommittedLeaderTail(t *testing.T) {
	manager, local := newTestManager(t, 2, 2, nil)
	if err := local["broker-2"].partition.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := manager.AppendAt([]byte("leader-only"), 100)
	if !errors.Is(err, ErrInsufficientISR) {
		t.Fatalf("AppendAt() error = %v, want ErrInsufficientISR", err)
	}
	if result.Offset != 0 || result.HighWatermark != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if got := manager.HighWatermark(); got != 0 {
		t.Fatalf("HighWatermark() = %d, want 0", got)
	}
	leaderNext, err := local["broker-1"].partition.NextOffset()
	if err != nil {
		t.Fatal(err)
	}
	if leaderNext != 1 {
		t.Fatalf("leader next offset = %d, want 1", leaderNext)
	}
}

func TestReplicationBatchFetchEncodeApplyAndDuplicateRetry(t *testing.T) {
	manager, local := newTestManager(t, 2, 1, nil)
	if err := manager.MarkReplicaUnavailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AppendAt([]byte("replicate-me"), 999); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaAvailable("broker-2"); err != nil {
		t.Fatal(err)
	}

	batch, err := manager.FetchReplicationBatch("broker-2", 0, 10, 1024)
	if err != nil {
		t.Fatalf("FetchReplicationBatch() error: %v", err)
	}
	if len(batch.Records) != 1 || batch.LeaderEpoch != 1 || batch.LeaderID != "broker-1" {
		t.Fatalf("unexpected batch: %+v", batch)
	}

	wire, err := EncodeReplicationBatch(batch)
	if err != nil {
		t.Fatalf("EncodeReplicationBatch() error: %v", err)
	}
	decoded, err := DecodeReplicationBatch(wire)
	if err != nil {
		t.Fatalf("DecodeReplicationBatch() error: %v", err)
	}
	if err := manager.ApplyReplicationBatch("broker-2", decoded); err != nil {
		t.Fatalf("ApplyReplicationBatch() error: %v", err)
	}
	// Delivery of the same batch twice is safe: the follower verifies the
	// existing payload and timestamp rather than appending a duplicate.
	if err := manager.ApplyReplicationBatch("broker-2", decoded); err != nil {
		t.Fatalf("duplicate ApplyReplicationBatch() error: %v", err)
	}

	if got := len(manager.InSyncReplicas()); got != 2 {
		t.Fatalf("ISR count = %d, want 2", got)
	}
	value, ts, err := local["broker-2"].partition.ReadWithTimestamp(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "replicate-me" || ts != 999 {
		t.Fatalf("follower record = (%q,%d)", value, ts)
	}
	next, err := local["broker-2"].partition.NextOffset()
	if err != nil {
		t.Fatal(err)
	}
	if next != 1 {
		t.Fatalf("follower next offset = %d, want 1", next)
	}
}

func TestApplyReplicationBatchRejectsStaleEpoch(t *testing.T) {
	manager, _ := newTestManager(t, 3, 1, nil)
	batch, err := manager.FetchReplicationBatch("broker-2", 0, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaUnavailable("broker-1"); err != nil {
		t.Fatal(err)
	}
	if err := manager.ElectLeader("broker-3"); err != nil {
		t.Fatalf("ElectLeader() error: %v", err)
	}
	if err := manager.MarkReplicaAvailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if err := manager.ApplyReplicationBatch("broker-2", batch); !errors.Is(err, ErrStaleLeaderEpoch) {
		t.Fatalf("ApplyReplicationBatch() error = %v, want ErrStaleLeaderEpoch", err)
	}
}

func TestCatchUpRejectsDivergentExistingFollowerData(t *testing.T) {
	manager, local := newTestManager(t, 2, 1, nil)
	if err := manager.MarkReplicaUnavailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AppendAt([]byte("leader-value"), 500); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaAvailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := local["broker-2"].partition.AppendAtSync([]byte("follower-value"), 500); err != nil {
		t.Fatal(err)
	}

	if err := manager.CatchUpReplica("broker-2"); !errors.Is(err, ErrDivergentReplica) {
		t.Fatalf("CatchUpReplica() error = %v, want ErrDivergentReplica", err)
	}
	if _, inISR := func() (struct{}, bool) {
		for _, id := range manager.InSyncReplicas() {
			if id == "broker-2" {
				return struct{}{}, true
			}
		}
		return struct{}{}, false
	}(); inISR {
		t.Fatal("divergent follower was added to ISR")
	}
}

func TestMarkReplicaUnavailablePreservesHighWatermark(t *testing.T) {
	manager, _ := newTestManager(t, 2, 1, nil)
	if _, err := manager.Append([]byte("committed")); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaUnavailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if got := manager.HighWatermark(); got != 1 {
		t.Fatalf("HighWatermark() decreased to %d, want 1", got)
	}
}

func TestElectLeaderPromotesCaughtUpISRReplica(t *testing.T) {
	manager, local := newTestManager(t, 3, 2, nil)
	for _, value := range []string{"one", "two"} {
		if _, err := manager.Append([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.MarkReplicaUnavailable("broker-1"); err != nil {
		t.Fatal(err)
	}
	if err := manager.ElectLeader("broker-2"); err != nil {
		t.Fatalf("ElectLeader() error: %v", err)
	}

	status := manager.Describe()
	if status.LeaderID != "broker-2" || status.LeaderEpoch != 2 {
		t.Fatalf("unexpected leader state: %+v", status)
	}
	if status.HighWatermark != 2 {
		t.Fatalf("HighWatermark = %d, want 2", status.HighWatermark)
	}
	if local["broker-1"].replica.Status().Role != RoleOffline {
		t.Fatal("old leader should be offline after promotion")
	}

	result, err := manager.Append([]byte("after-election"))
	if err != nil {
		t.Fatalf("append after election: %v", err)
	}
	if result.Offset != 2 || result.HighWatermark != 3 {
		t.Fatalf("unexpected post-election append: %+v", result)
	}
}

func TestElectLeaderRejectsNonISRReplica(t *testing.T) {
	manager, _ := newTestManager(t, 2, 1, nil)
	if err := manager.MarkReplicaUnavailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaUnavailable("broker-1"); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaAvailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if err := manager.ElectLeader("broker-2"); !errors.Is(err, ErrNoEligibleLeader) {
		t.Fatalf("ElectLeader() error = %v, want ErrNoEligibleLeader", err)
	}
}

func TestElectionDoesNotPromoteUncommittedLeaderTail(t *testing.T) {
	manager, local := newTestManager(t, 2, 1, nil)
	if _, err := local["broker-1"].partition.AppendAtSync([]byte("uncommitted"), 777); err != nil {
		t.Fatal(err)
	}
	if manager.HighWatermark() != 0 {
		t.Fatal("unexpected high watermark before election")
	}
	if err := manager.MarkReplicaUnavailable("broker-1"); err != nil {
		t.Fatal(err)
	}
	if err := manager.ElectLeader("broker-2"); err != nil {
		t.Fatalf("ElectLeader() error: %v", err)
	}

	read, err := manager.ReadCommitted(0, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != 0 {
		t.Fatalf("uncommitted old leader data was promoted: %+v", read)
	}
	result, err := manager.Append([]byte("new-leader-record"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Offset != 0 {
		t.Fatalf("new leader offset = %d, want 0", result.Offset)
	}
}

func TestReadCommittedHonorsRecordAndByteLimits(t *testing.T) {
	manager, _ := newTestManager(t, 2, 2, nil)
	for _, value := range []string{"aaa", "bbb", "ccc"} {
		if _, err := manager.Append([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	batch, err := manager.ReadCommitted(0, 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 2 || batch.NextOffset != 2 {
		t.Fatalf("record limit not applied: %+v", batch)
	}

	byteLimited, err := manager.ReadCommitted(0, 10, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(byteLimited.Records) != 1 || byteLimited.NextOffset != 1 {
		t.Fatalf("byte limit not applied: %+v", byteLimited)
	}
	if _, err := manager.ReadCommitted(0, 10, 2); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversized first record error = %v, want ErrRecordTooLarge", err)
	}
}

func TestCatchUpRejectsReplicaBehindLeaderRetention(t *testing.T) {
	seed := []Record{
		{Offset: 0, Timestamp: 1, Value: []byte("first!")},
		{Offset: 1, Timestamp: 2, Value: []byte("second")},
	}
	leader := openTestReplica(t, "broker-1", "orders", 10, seed)
	follower := openTestReplica(t, "broker-2", "orders", 10, seed)
	manager, err := NewManager([]*Replica{leader.replica, follower.replica}, Config{LeaderID: "broker-1", MinISR: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaUnavailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"third!", "fourth"} {
		if _, err := manager.Append([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := leader.partition.ApplyRetention(storage.RetentionConfig{MaxSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaAvailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if err := manager.CatchUpReplica("broker-2"); !errors.Is(err, ErrReplicaBehindRetention) {
		t.Fatalf("CatchUpReplica() error = %v, want ErrReplicaBehindRetention", err)
	}
}

func TestConcurrentLeaderAppendsProduceUniqueOffsets(t *testing.T) {
	manager, local := newTestManager(t, 2, 2, nil)
	const workers = 40
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan AppendResult, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			result, err := manager.Append([]byte(fmt.Sprintf("value-%d", n)))
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("Append() error: %v", err)
	}
	seen := make([]bool, workers)
	count := 0
	for result := range results {
		if result.Offset >= workers {
			t.Errorf("offset %d out of range", result.Offset)
			continue
		}
		if seen[result.Offset] {
			t.Errorf("duplicate offset %d", result.Offset)
		}
		seen[result.Offset] = true
		count++
	}
	if count != workers {
		t.Fatalf("successful appends = %d, want %d", count, workers)
	}
	if got := manager.HighWatermark(); got != workers {
		t.Fatalf("HighWatermark() = %d, want %d", got, workers)
	}
	for id, replica := range local {
		next, err := replica.partition.NextOffset()
		if err != nil {
			t.Fatal(err)
		}
		if next != workers {
			t.Errorf("replica %s next offset = %d, want %d", id, next, workers)
		}
	}
}

func TestReplicaAccessorsAndManagerDiagnostics(t *testing.T) {
	manager, local := newTestManager(t, 2, 1, nil)
	replica := local["broker-2"].replica
	if replica.ID() != "broker-2" || replica.TopicName() != "orders" || replica.PartitionID() != 0 {
		t.Fatalf("unexpected replica identity: id=%q topic=%q partition=%d", replica.ID(), replica.TopicName(), replica.PartitionID())
	}
	if replica.Status().Role != RoleFollower || !replica.Status().Available {
		t.Fatalf("unexpected initial replica status: %+v", replica.Status())
	}
	if RoleFollower.String() != "follower" || RoleLeader.String() != "leader" || RoleOffline.String() != "offline" || Role(99).String() != "unknown" {
		t.Fatal("Role.String() returned an unexpected value")
	}
	resolved, err := manager.Replica("broker-2")
	if err != nil || resolved != replica {
		t.Fatalf("Replica() = (%p,%v), want (%p,nil)", resolved, err, replica)
	}
	if _, err := manager.Replica("missing"); !errors.Is(err, ErrReplicaNotFound) {
		t.Fatalf("Replica(missing) error = %v", err)
	}
	name, id := manager.TopicPartition()
	if name != "orders" || id != 0 {
		t.Fatalf("TopicPartition() = (%q,%d)", name, id)
	}
	if err := manager.AssertPartition(local["broker-2"].partition); err != nil {
		t.Fatalf("AssertPartition() error: %v", err)
	}
	other := openTestReplica(t, "broker-x", "payments", 1024, nil)
	if err := manager.AssertPartition(other.partition); !errors.Is(err, ErrPartitionMismatch) {
		t.Fatalf("AssertPartition(other) error = %v, want ErrPartitionMismatch", err)
	}
	if got := manager.Describe(); len(got.Replicas) != 2 || got.LeaderID != "broker-1" {
		t.Fatalf("unexpected Describe(): %+v", got)
	}
}

func TestReplicaApplyReplicationRejectsGapDivergenceAndStaleEpoch(t *testing.T) {
	local := openTestReplica(t, "broker-2", "orders", 1024, nil)
	record := Record{Offset: 0, Timestamp: 50, Value: []byte("value")}
	if err := local.replica.ApplyReplication("broker-1", 2, Record{Offset: 1, Timestamp: 50, Value: []byte("gap")}); !errors.Is(err, ErrReplicationGap) {
		t.Fatalf("gap error = %v, want ErrReplicationGap", err)
	}
	if err := local.replica.ApplyReplication("broker-1", 2, record); err != nil {
		t.Fatalf("ApplyReplication() error: %v", err)
	}
	if err := local.replica.ApplyReplication("broker-1", 2, record); err != nil {
		t.Fatalf("identical duplicate error: %v", err)
	}
	if err := local.replica.ApplyReplication("broker-1", 2, Record{Offset: 0, Timestamp: 50, Value: []byte("different")}); !errors.Is(err, ErrDivergentReplica) {
		t.Fatalf("divergent duplicate error = %v, want ErrDivergentReplica", err)
	}
	if err := local.replica.ApplyReplication("broker-1", 1, record); !errors.Is(err, ErrStaleLeaderEpoch) {
		t.Fatalf("stale epoch error = %v, want ErrStaleLeaderEpoch", err)
	}
	if err := local.replica.ApplyReplication("broker-2", 2, record); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("self replication error = %v, want ErrNotLeader", err)
	}
}

func TestManagerRejectsInvalidConfiguration(t *testing.T) {
	first := openTestReplica(t, "broker-1", "orders", 1024, nil)
	second := openTestReplica(t, "broker-2", "orders", 1024, nil)

	tests := []struct {
		name      string
		replicas  []*Replica
		config    Config
		wantError error
	}{
		{name: "no replicas", replicas: nil, config: Config{LeaderID: "broker-1"}, wantError: ErrInvalidConfig},
		{name: "missing leader", replicas: []*Replica{second.replica}, config: Config{LeaderID: "broker-1"}, wantError: ErrReplicaNotFound},
		{name: "minimum ISR too high", replicas: []*Replica{first.replica, second.replica}, config: Config{LeaderID: "broker-1", MinISR: 3}, wantError: ErrInvalidConfig},
		{name: "negative batch count", replicas: []*Replica{first.replica, second.replica}, config: Config{LeaderID: "broker-1", MinISR: 1, ReplicationBatchRecords: -1}, wantError: ErrInvalidConfig},
		{name: "oversized batch bytes", replicas: []*Replica{first.replica, second.replica}, config: Config{LeaderID: "broker-1", MinISR: 1, ReplicationBatchBytes: MaxReplicationFrameBytes + 1}, wantError: ErrInvalidConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewManager(tt.replicas, tt.config)
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("NewManager() error = %v, want %v", err, tt.wantError)
			}
		})
	}
	duplicate, err := NewReplica("broker-1", second.partition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewManager([]*Replica{first.replica, duplicate}, Config{LeaderID: "broker-1", MinISR: 1}); !errors.Is(err, ErrDuplicateReplica) {
		t.Fatalf("duplicate ID error = %v, want ErrDuplicateReplica", err)
	}
	if _, err := NewReplica("bad id", first.partition); !errors.Is(err, ErrInvalidReplicaID) {
		t.Fatalf("invalid replica ID error = %v, want ErrInvalidReplicaID", err)
	}
}

func TestFetchReplicationBatchRejectsInvalidRequests(t *testing.T) {
	manager, local := newTestManager(t, 2, 1, nil)
	if _, err := manager.FetchReplicationBatch("missing", 0, 10, 100); !errors.Is(err, ErrReplicaNotFound) {
		t.Fatalf("unknown follower error = %v", err)
	}
	if _, err := manager.FetchReplicationBatch("broker-1", 0, 10, 100); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("leader fetch error = %v", err)
	}
	if _, err := manager.FetchReplicationBatch("broker-2", 0, 0, 100); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero record limit error = %v", err)
	}
	if _, err := manager.FetchReplicationBatch("broker-2", 0, 10, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero byte limit error = %v", err)
	}
	if _, err := manager.FetchReplicationBatch("broker-2", 1, 10, 100); !errors.Is(err, ErrReplicationGap) {
		t.Fatalf("offset beyond leader end error = %v", err)
	}
	if _, err := manager.Append([]byte("long-message")); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.FetchReplicationBatch("broker-2", 0, 10, 2); !errors.Is(err, ErrInvalidReplicationBatch) {
		t.Fatalf("record larger than batch limit error = %v", err)
	}
	if err := manager.MarkReplicaUnavailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.FetchReplicationBatch("broker-2", 0, 10, 100); !errors.Is(err, ErrReplicaUnavailable) {
		t.Fatalf("unavailable follower error = %v", err)
	}
	_ = local
}

func TestApplyReplicationBatchRejectsWrongIdentityAndMissingReplica(t *testing.T) {
	manager, _ := newTestManager(t, 2, 1, nil)
	batch := ReplicationBatch{
		Version: ProtocolVersion, Topic: "orders", PartitionID: 0,
		LeaderID: "broker-1", LeaderEpoch: 1,
	}
	if err := manager.ApplyReplicationBatch("missing", batch); !errors.Is(err, ErrReplicaNotFound) {
		t.Fatalf("missing follower error = %v", err)
	}
	wrongTopic := batch
	wrongTopic.Topic = "payments"
	if err := manager.ApplyReplicationBatch("broker-2", wrongTopic); !errors.Is(err, ErrPartitionMismatch) {
		t.Fatalf("wrong topic error = %v", err)
	}
	wrongPartition := batch
	wrongPartition.PartitionID = 2
	if err := manager.ApplyReplicationBatch("broker-2", wrongPartition); !errors.Is(err, ErrPartitionMismatch) {
		t.Fatalf("wrong partition error = %v", err)
	}
	if err := manager.ApplyReplicationBatch("broker-1", batch); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("apply to leader error = %v", err)
	}
}

func TestCatchUpRejectsFollowerAheadOfLeader(t *testing.T) {
	manager, local := newTestManager(t, 2, 1, nil)
	if err := manager.MarkReplicaUnavailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := local["broker-2"].partition.AppendAtSync([]byte("orphan"), 10); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaAvailable("broker-2"); err != nil {
		t.Fatal(err)
	}
	if err := manager.CatchUpReplica("broker-2"); !errors.Is(err, ErrReplicaAheadOfLeader) {
		t.Fatalf("CatchUpReplica() error = %v, want ErrReplicaAheadOfLeader", err)
	}
}

func TestMarkReplicaAvailableCannotRestartFailedLeaderDirectly(t *testing.T) {
	manager, _ := newTestManager(t, 2, 1, nil)
	if err := manager.MarkReplicaUnavailable("broker-1"); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaAvailable("broker-1"); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("MarkReplicaAvailable(leader) error = %v, want ErrNoLeader", err)
	}
}

func TestElectLeaderRejectsCandidateWithUncommittedTail(t *testing.T) {
	manager, local := newTestManager(t, 2, 1, nil)
	if _, err := local["broker-2"].partition.AppendAtSync([]byte("follower-tail"), 77); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkReplicaUnavailable("broker-1"); err != nil {
		t.Fatal(err)
	}
	if err := manager.ElectLeader("broker-2"); !errors.Is(err, ErrReplicaNotCaughtUp) {
		t.Fatalf("ElectLeader() error = %v, want ErrReplicaNotCaughtUp", err)
	}
}

func TestReadCommittedRejectsInvalidLimitsAndRetainedOffset(t *testing.T) {
	leader := openTestReplica(t, "broker-1", "orders", 10, nil)
	follower := openTestReplica(t, "broker-2", "orders", 10, nil)
	manager, err := NewManager([]*Replica{leader.replica, follower.replica}, Config{LeaderID: "broker-1", MinISR: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ReadCommitted(0, 0, 100); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero record limit error = %v", err)
	}
	if _, err := manager.ReadCommitted(0, 1, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero byte limit error = %v", err)
	}
	for _, value := range []string{"first!", "second", "third!"} {
		if _, err := manager.Append([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := leader.partition.ApplyRetention(storage.RetentionConfig{MaxSize: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ReadCommitted(0, 10, 100); !errors.Is(err, ErrOffsetBeforeHighWatermark) {
		t.Fatalf("read before retained start error = %v", err)
	}
}

func TestReplicaDuplicateOlderRecordDoesNotRegressMatchOffset(t *testing.T) {
	local := openTestReplica(t, "broker-2", "orders", 1024, nil)

	records := []Record{
		{Offset: 0, Timestamp: 10, Value: []byte("first")},
		{Offset: 1, Timestamp: 20, Value: []byte("second")},
	}
	for _, record := range records {
		if err := local.replica.ApplyReplication("broker-1", 1, record); err != nil {
			t.Fatalf("ApplyReplication(%d) error: %v", record.Offset, err)
		}
	}

	if got := local.replica.Status().MatchOffset; got != 2 {
		t.Fatalf("MatchOffset before duplicate = %d, want 2", got)
	}

	if err := local.replica.ApplyReplication("broker-1", 1, records[0]); err != nil {
		t.Fatalf("duplicate ApplyReplication() error: %v", err)
	}

	if got := local.replica.Status().MatchOffset; got != 2 {
		t.Fatalf("MatchOffset after older duplicate = %d, want 2", got)
	}
}
