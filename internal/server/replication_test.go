package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/replication"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func TestParseReplicaAssignments(t *testing.T) {
	assignments, err := parseReplicaAssignments("orders:0=broker-1,broker-2,broker-3;orders:1=broker-2,broker-3,broker-1")
	if err != nil {
		t.Fatal(err)
	}
	first := assignments[partitionKey{Topic: "orders", PartitionID: 0}]
	if first.LeaderID != "broker-1" || len(first.Replicas) != 3 || first.LeaderEpoch != 1 {
		t.Fatalf("unexpected first assignment: %+v", first)
	}
	second := assignments[partitionKey{Topic: "orders", PartitionID: 1}]
	if second.LeaderID != "broker-2" {
		t.Fatalf("second leader = %q, want broker-2", second.LeaderID)
	}
}

func TestParseReplicaAssignmentsRejectsInvalidInput(t *testing.T) {
	cases := []string{
		"orders=broker-1",
		"orders:x=broker-1",
		"orders:0=",
		"orders:0=broker-1,broker-1",
		"orders:0=broker-1;orders:0=broker-2",
		"bad topic:0=broker-1",
		"orders:0=broker-1;",
	}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			if _, err := parseReplicaAssignments(input); err == nil {
				t.Fatalf("parseReplicaAssignments(%q) unexpectedly succeeded", input)
			}
		})
	}
}

func TestValidateReplicaAssignmentsRequiresCompleteCatalog(t *testing.T) {
	assignments, err := parseReplicaAssignments("orders:0=broker-1,broker-2;orders:1=broker-2,broker-1")
	if err != nil {
		t.Fatal(err)
	}
	definitions := []TopicDefinition{{Name: "orders", Partitions: 2}}
	if err := validateReplicaAssignments(definitions, assignments, 2); err != nil {
		t.Fatalf("valid assignment rejected: %v", err)
	}
	if err := validateReplicaAssignments(definitions, assignments, 3); err == nil {
		t.Fatal("min ISR larger than replica count was accepted")
	}
	if err := validateReplicaAssignments([]TopicDefinition{{Name: "orders", Partitions: 3}}, assignments, 1); err == nil {
		t.Fatal("missing partition assignment was accepted")
	}
	if err := validateReplicaAssignments(definitions, assignments, 0); err == nil {
		t.Fatal("zero min ISR was accepted")
	}
}

func TestApplyReplicationBatchIsDurableAndIdempotent(t *testing.T) {
	registry := topic.NewRegistry()
	registered, err := registry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := registered.OpenLogs(filepath.Join(t.TempDir(), "logs"), 1<<20); err != nil {
		t.Fatal(err)
	}
	defer registered.CloseLogs()
	partition, err := registered.Partition(0)
	if err != nil {
		t.Fatal(err)
	}
	batch := replication.ReplicationBatch{
		Version: replication.ProtocolVersion, Topic: "orders", PartitionID: 0,
		LeaderID: "broker-1", LeaderEpoch: 1,
		Records: []replication.Record{{Offset: 0, Timestamp: 123, Value: []byte("record-0")}, {Offset: 1, Timestamp: 124, Value: []byte("record-1")}},
	}
	if err := applyReplicationBatch(context.Background(), partition, batch); err != nil {
		t.Fatal(err)
	}
	if err := applyReplicationBatch(context.Background(), partition, batch); err != nil {
		t.Fatalf("identical replay should be idempotent: %v", err)
	}
	start, next, err := partition.OffsetRange()
	if err != nil {
		t.Fatal(err)
	}
	if start != 0 || next != 2 {
		t.Fatalf("range = [%d,%d), want [0,2)", start, next)
	}
	divergent := batch
	divergent.Records = []replication.Record{{Offset: 1, Timestamp: 999, Value: []byte("different")}}
	if err := applyReplicationBatch(context.Background(), partition, divergent); !errors.Is(err, replication.ErrDivergentReplica) {
		t.Fatalf("divergent replay error = %v, want ErrDivergentReplica", err)
	}
	gap := batch
	gap.Records = []replication.Record{{Offset: 4, Timestamp: 125, Value: []byte("gap")}}
	if err := applyReplicationBatch(context.Background(), partition, gap); !errors.Is(err, replication.ErrReplicationGap) {
		t.Fatalf("gap error = %v, want ErrReplicationGap", err)
	}
}

func TestApplyReplicationBatchHonorsContext(t *testing.T) {
	registry := topic.NewRegistry()
	registered, err := registry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := registered.OpenLogs(filepath.Join(t.TempDir(), "logs"), 1<<20); err != nil {
		t.Fatal(err)
	}
	defer registered.CloseLogs()
	partition, _ := registered.Partition(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	batch := replication.ReplicationBatch{
		Version: replication.ProtocolVersion, Topic: "orders", PartitionID: 0,
		LeaderID: "broker-1", LeaderEpoch: 1,
		Records: []replication.Record{{Offset: 0, Timestamp: 1, Value: []byte("x")}},
	}
	if err := applyReplicationBatch(ctx, partition, batch); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestAssignedLeaderAckRemainsHiddenUntilCommitWatermarkAdvances(t *testing.T) {
	cfg := testConfig(filepath.Join(t.TempDir(), "broker-data"))
	cfg.ClusterID = "commit-boundary-test"
	cfg.PeerListenAddress = "127.0.0.1:29191"
	cfg.PeerAdvertiseAddress = "127.0.0.1:29191"
	cfg.ReplicaAssignments = "orders:0=test-broker,broker-2"
	cfg.MinISR = 2
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runtime, err := New(Options{Config: cfg, BootstrapTopics: []TopicDefinition{{Name: "orders", Partitions: 1}}, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTopics(runtime.topics)

	producePayload, err := protocol.EncodeProduceRequest(protocol.ProduceRequest{
		AckMode: protocol.AckLeader,
		Batch:   protocol.MessageBatch{Messages: []protocol.Message{{Topic: "orders", Key: []byte("k"), Value: []byte("pending"), Timestamp: 123}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	produceResponse, err := runtime.dispatch.Dispatch(protocol.Request{APIKey: protocol.APIPRODUCE, APIVersion: 1, Payload: producePayload})
	if err != nil {
		t.Fatal(err)
	}
	appendResult, err := protocol.DecodeProduceResponse(produceResponse)
	if err != nil || len(appendResult.Records) != 1 {
		t.Fatalf("decode produce response records=%+v err=%v", appendResult.Records, err)
	}
	state, exists := runtime.commits.Get("orders", 0)
	if !exists || state.HighWatermark != 0 {
		t.Fatalf("acks=leader advanced commit state: %+v exists=%t", state, exists)
	}

	consumePayload, err := protocol.EncodeConsumeRequest(protocol.ConsumeRequest{Topic: "orders", PartitionID: 0, Offset: 0, MaxMessages: 10})
	if err != nil {
		t.Fatal(err)
	}
	consumeResponse, err := runtime.dispatch.Dispatch(protocol.Request{APIKey: protocol.APICONSUME, APIVersion: 2, Payload: consumePayload})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := protocol.DecodeConsumeResponseV2(consumeResponse)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Records) != 0 || pending.LogEndOffset != 0 || pending.NextOffset != 0 {
		t.Fatalf("uncommitted record was visible: %+v", pending)
	}

	if _, err := runtime.commits.Advance("orders", 0, 1, "test-broker", 1, 1); err != nil {
		t.Fatal(err)
	}
	consumeResponse, err = runtime.dispatch.Dispatch(protocol.Request{APIKey: protocol.APICONSUME, APIVersion: 2, Payload: consumePayload})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := protocol.DecodeConsumeResponseV2(consumeResponse)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed.Records) != 1 || string(committed.Records[0].Value) != "pending" || committed.LogEndOffset != 1 {
		t.Fatalf("record did not become visible after commit: %+v", committed)
	}
}
