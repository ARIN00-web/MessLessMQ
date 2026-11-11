package membership

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/interbroker"
)

func startMembershipServer(t *testing.T, clusterID, brokerID string) (*Registry, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistry(clusterID, Broker{ID: brokerID, ClientAddress: "127.0.0.1:9092", PeerAddress: listener.Addr().String()})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	handler, err := NewHandler(registry)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	mux := interbroker.NewMux()
	if err := mux.Register(interbroker.MessageBrokerJoin, handler); err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	server, err := interbroker.NewServer(mux)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("membership server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("membership server did not stop")
		}
	})
	return registry, listener.Addr().String()
}

func testBroker(id, peerAddress string) Broker {
	return Broker{ID: id, ClientAddress: "127.0.0.1:9092", PeerAddress: peerAddress}
}

func TestThreeBrokerDiscoveryFormsFullMembershipView(t *testing.T) {
	first, firstAddress := startMembershipServer(t, "test-cluster", "broker-1")
	second, _ := startMembershipServer(t, "test-cluster", "broker-2")
	third, _ := startMembershipServer(t, "test-cluster", "broker-3")

	transport := interbroker.NewTransport(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := second.Join(ctx, transport, []Seed{{ID: "broker-1", Address: firstAddress}}); err != nil {
		t.Fatal(err)
	}
	if err := third.Join(ctx, transport, []Seed{{ID: "broker-1", Address: firstAddress}}); err != nil {
		t.Fatal(err)
	}

	for name, registry := range map[string]*Registry{"broker-1": first, "broker-2": second, "broker-3": third} {
		snapshot := registry.Snapshot()
		if len(snapshot.Brokers) != 3 {
			t.Fatalf("%s discovered %d brokers: %+v", name, len(snapshot.Brokers), snapshot.Brokers)
		}
		if snapshot.ClusterID != "test-cluster" || snapshot.Version != 3 {
			t.Fatalf("%s snapshot metadata = %+v", name, snapshot)
		}
	}
}

func TestJoinRejectsDifferentClusterID(t *testing.T) {
	first, address := startMembershipServer(t, "cluster-a", "broker-a")
	second, err := NewRegistry("cluster-b", testBroker("broker-b", "127.0.0.1:19092"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = second.Join(ctx, interbroker.NewTransport(time.Second), []Seed{{ID: "broker-a", Address: address}})
	if !errors.Is(err, ErrNoSeedReachable) || !strings.Contains(err.Error(), ErrClusterMismatch.Error()) {
		t.Fatalf("Join error = %v, want ErrNoSeedReachable with cluster mismatch detail", err)
	}
	if got := len(first.Snapshot().Brokers); got != 1 {
		t.Fatalf("foreign broker was registered: count=%d", got)
	}
}

func TestJoinWithOnlySelfSeedActsAsBootstrap(t *testing.T) {
	registry, err := NewRegistry("test-cluster", Broker{
		ID: "broker-1", ClientAddress: "127.0.0.1:9092", PeerAddress: "127.0.0.1:19093",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := registry.Join(ctx, interbroker.NewTransport(time.Second), []Seed{{ID: "broker-1", Address: "127.0.0.1:19093"}}); err != nil {
		t.Fatalf("self seed should be ignored for bootstrap: %v", err)
	}
	snapshot := registry.Snapshot()
	if len(snapshot.Brokers) != 1 || snapshot.Brokers[0].ID != "broker-1" {
		t.Fatalf("unexpected bootstrap snapshot: %+v", snapshot)
	}
}

func TestRegisterRejectsAddressConflict(t *testing.T) {
	registry, err := NewRegistry("cluster-a", testBroker("broker-a", "127.0.0.1:19091"))
	if err != nil {
		t.Fatal(err)
	}
	conflict := testBroker("broker-a", "127.0.0.1:29091")
	if err := registry.Register("cluster-a", conflict); !errors.Is(err, ErrBrokerConflict) {
		t.Fatalf("Register error = %v, want ErrBrokerConflict", err)
	}
}

func TestParseSeeds(t *testing.T) {
	seeds, err := ParseSeeds([]string{"broker-1@127.0.0.1:19091", " broker-2@localhost:19092 "})
	if err != nil {
		t.Fatal(err)
	}
	if len(seeds) != 2 || seeds[0].ID != "broker-1" || seeds[1].Address != "localhost:19092" {
		t.Fatalf("unexpected seeds: %+v", seeds)
	}
	if _, err := ParseSeeds([]string{"invalid-seed"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ParseSeeds error = %v", err)
	}
	if _, err := ParseSeeds([]string{"broker-1@:19091"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty-host seed error = %v", err)
	}
	if _, err := ParseSeeds([]string{"broker-1@localhost:70000"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("out-of-range seed port error = %v", err)
	}
	if _, err := ParseSeeds([]string{"broker-1@127.0.0.1:19091", "broker-1@127.0.0.1:19092"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("duplicate ParseSeeds error = %v", err)
	}
}
