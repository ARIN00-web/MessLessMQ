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
