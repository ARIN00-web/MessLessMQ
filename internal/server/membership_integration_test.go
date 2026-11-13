package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/config"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/recordmeta"
)

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForTCP(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("address %s did not become reachable", address)
}

func startClusterRuntime(t *testing.T, cfg config.Config, seedTopics []TopicDefinition) *Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runtime, err := New(Options{Config: cfg, BootstrapTopics: seedTopics, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	waitForTCP(t, cfg.ListenAddress)
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("runtime %s shutdown: %v", cfg.BrokerID, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("runtime %s did not stop", cfg.BrokerID)
		}
	})
	return runtime
}

func clusterConfig(t *testing.T, dataDir, id string, seeds ...string) config.Config {
	t.Helper()
	cfg := config.Defaults()
	cfg.BrokerID = id
	cfg.ListenAddress = reserveAddress(t)
	cfg.AdvertiseAddress = cfg.ListenAddress
	cfg.DataDir = filepath.Join(dataDir, id)
	cfg.ClusterID = "integration-cluster"
	cfg.PeerListenAddress = reserveAddress(t)
	cfg.PeerAdvertiseAddress = cfg.PeerListenAddress
	cfg.SeedPeers = seeds
	cfg.PeerJoinTimeout = 3 * time.Second
	cfg.RequestTimeout = 2 * time.Second
	cfg.ShutdownTimeout = 2 * time.Second
	return cfg
}

func TestServerPinsPersistentClusterAndBrokerIdentity(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "broker-data")
	cfg := testConfig(dataDir)
	cfg.ClusterID = "integration-cluster"
	cfg.PeerListenAddress = "127.0.0.1:19091"
	cfg.PeerAdvertiseAddress = "127.0.0.1:19091"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	runtime, err := New(Options{Config: cfg, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	if err := closeTopics(runtime.topics); err != nil {
		t.Fatal(err)
	}

	changedCluster := cfg
	changedCluster.ClusterID = "different-cluster"
	if _, err := New(Options{Config: changedCluster, Logger: logger}); err == nil {
		t.Fatal("reusing a data directory in another cluster should fail")
	}

	changedBroker := cfg
	changedBroker.BrokerID = "broker-2"
	if _, err := New(Options{Config: changedBroker, Logger: logger}); err == nil {
		t.Fatal("reusing a data directory under another broker ID should fail")
	}
}

func TestThreeServerProcessesDiscoverMembershipAndAdvertiseBrokers(t *testing.T) {
	root := t.TempDir()
	firstConfig := clusterConfig(t, root, "broker-1")
	first := startClusterRuntime(t, firstConfig, []TopicDefinition{{Name: "orders", Partitions: 1}})

	secondConfig := clusterConfig(t, root, "broker-2", "broker-1@"+firstConfig.PeerAdvertiseAddress)
	second := startClusterRuntime(t, secondConfig, []TopicDefinition{{Name: "orders", Partitions: 1}})

	thirdConfig := clusterConfig(t, root, "broker-3", "broker-1@"+firstConfig.PeerAdvertiseAddress)
	third := startClusterRuntime(t, thirdConfig, []TopicDefinition{{Name: "orders", Partitions: 1}})

	for id, runtime := range map[string]*Server{"broker-1": first, "broker-2": second, "broker-3": third} {
		snapshot := runtime.Membership().Snapshot()
		if snapshot.ClusterID != "integration-cluster" || len(snapshot.Brokers) != 3 {
			t.Fatalf("%s membership snapshot = %+v", id, snapshot)
		}
	}

	conn, err := net.DialTimeout("tcp", secondConfig.ListenAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	payload, err := protocol.EncodeMetadataRequest(protocol.MetadataRequest{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	response := exchange(t, conn, protocol.Request{APIKey: protocol.APIMETADATA, APIVersion: protocol.MetadataAPIVersion, CorrelationID: 99, Payload: payload})
	if response.ErrorCode != protocol.ErrorNone {
		t.Fatalf("metadata response error code=%d", response.ErrorCode)
	}
	metadata, err := protocol.DecodeMetadataResponse(response.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.Brokers) != 3 {
		t.Fatalf("metadata contains %d brokers, want 3: %+v", len(metadata.Brokers), metadata.Brokers)
	}
	if len(metadata.Partitions) != 1 || metadata.Partitions[0].Leader != "broker-2" || len(metadata.Partitions[0].Replicas) != 1 || metadata.Partitions[0].Replicas[0] != "broker-2" {
		t.Fatalf("unexpected local partition metadata: %+v", metadata.Partitions)
	}
}

func TestThreeServerProcessesCommitBrokerAndPartitionMetadata(t *testing.T) {
	root := t.TempDir()
	voters := []string{"broker-1", "broker-2", "broker-3"}
	firstConfig := clusterConfig(t, root, "broker-1")
	firstConfig.ControllerVoters = voters
	first := startClusterRuntime(t, firstConfig, []TopicDefinition{{Name: "orders", Partitions: 3}})

	secondConfig := clusterConfig(t, root, "broker-2", "broker-1@"+firstConfig.PeerAdvertiseAddress)
	secondConfig.ControllerVoters = voters
	second := startClusterRuntime(t, secondConfig, []TopicDefinition{{Name: "orders", Partitions: 3}})

	thirdConfig := clusterConfig(t, root, "broker-3", "broker-1@"+firstConfig.PeerAdvertiseAddress, "broker-2@"+secondConfig.PeerAdvertiseAddress)
	thirdConfig.ControllerVoters = voters
	third := startClusterRuntime(t, thirdConfig, []TopicDefinition{{Name: "orders", Partitions: 3}})

	servers := map[string]*Server{"broker-1": first, "broker-2": second, "broker-3": third}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for id, runtime := range servers {
			metadata := runtime.controllerNode.Metadata()
			if len(metadata.Brokers) != 3 || len(metadata.Partitions) != 3 || len(runtime.assignmentStore.snapshot()) != 3 {
				ready = false
				break
			}
			for _, brokerID := range voters {
				if _, exists := metadata.Brokers[brokerID]; !exists {
					ready = false
					break
				}
			}
			if !ready {
				_ = id
				break
			}
		}
		if ready {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for id, runtime := range servers {
		metadata := runtime.controllerNode.Metadata()
		if len(metadata.Brokers) != 3 || len(metadata.Partitions) != 3 {
			t.Fatalf("%s did not apply committed cluster metadata: brokers=%d partitions=%d status=%+v", id, len(metadata.Brokers), len(metadata.Partitions), runtime.controllerNode.Status())
		}
		if len(runtime.assignmentStore.snapshot()) != 3 {
			t.Fatalf("%s did not publish committed assignments to data plane", id)
		}
	}

	conn, err := net.DialTimeout("tcp", secondConfig.ListenAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	metadataPayload, err := protocol.EncodeMetadataRequest(protocol.MetadataRequest{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	metadataResponse := exchange(t, conn, protocol.Request{APIKey: protocol.APIMETADATA, APIVersion: protocol.MetadataAPIVersion, CorrelationID: 501, Payload: metadataPayload})
	if metadataResponse.ErrorCode != protocol.ErrorNone {
		t.Fatalf("metadata API returned error %d", metadataResponse.ErrorCode)
	}
	metadata, err := protocol.DecodeMetadataResponse(metadataResponse.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.Brokers) != 3 || len(metadata.Partitions) != 3 {
		t.Fatalf("metadata API did not expose committed view: %+v", metadata)
	}

	producePayload, err := protocol.EncodeProduceRequest(protocol.ProduceRequest{
		AckMode: protocol.AckAll,
		Batch:   protocol.MessageBatch{Messages: []protocol.Message{{Topic: "orders", Key: []byte("customer-7"), Value: []byte("controller-committed-order"), Timestamp: 22334}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	leaderConn, err := net.DialTimeout("tcp", firstConfig.ListenAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer leaderConn.Close()
	_ = leaderConn.SetDeadline(time.Now().Add(5 * time.Second))
	produceResponse := exchange(t, leaderConn, protocol.Request{APIKey: protocol.APIPRODUCE, APIVersion: 1, CorrelationID: 502, Payload: producePayload})
	if produceResponse.ErrorCode != protocol.ErrorNone {
		t.Fatalf("produce through committed partition leader failed with code %d", produceResponse.ErrorCode)
	}
	appendResult, err := protocol.DecodeProduceResponse(produceResponse.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(appendResult.Records) != 1 || appendResult.Records[0].PartitionID != 0 || appendResult.Records[0].Offset != 0 {
		t.Fatalf("unexpected append result: %+v", appendResult)
	}

	for id, runtime := range map[string]*Server{"broker-2": second, "broker-3": third} {
		partitionTopic, err := runtime.registry.Get("orders")
		if err != nil {
			t.Fatal(err)
		}
		partition, err := partitionTopic.Partition(0)
		if err != nil {
			t.Fatal(err)
		}
		stored, _, err := partition.ReadWithTimestamp(0)
		if err != nil {
			t.Fatalf("%s did not receive replicated record: %v", id, err)
		}
		decodedKey, decodedValue, _, err := recordmeta.DecodeMessage(stored)
		if err != nil {
			t.Fatal(err)
		}
		if string(decodedKey) != "customer-7" || string(decodedValue) != "controller-committed-order" {
			t.Fatalf("%s has wrong replicated record: %+v", id, decodedValue)
		}
	}
}
