package server

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/membership"
)

func TestControllerFollowerAuthorityLeaseExpires(t *testing.T) {
	dataDir := t.TempDir()
	node, err := controller.OpenDurableNode("broker-1", []string{"broker-2"}, filepath.Join(dataDir, "raft"))
	if err != nil {
		t.Fatal(err)
	}
	members, err := membership.NewRegistry("lease-test", membership.Broker{ID: "broker-1", ClientAddress: "127.0.0.1:9092", PeerAddress: "127.0.0.1:9192"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(filepath.Join(dataDir, "broker"))
	cfg.BrokerID = "broker-1"
	cfg.RequestTimeout = time.Second
	cfg.HeartbeatInterval = 100 * time.Millisecond
	cfg.ElectionTimeoutMin = 800 * time.Millisecond
	cfg.ElectionTimeoutMax = time.Second
	runtime, err := newControllerRuntime("broker-1", []string{"broker-1", "broker-2"}, node, members, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.HasAuthority() {
		t.Fatal("follower without a leader contact unexpectedly has authority")
	}
	response, err := node.HandleAppendEntries(controller.AppendEntriesRequest{Term: 1, LeaderID: "broker-2"})
	if err != nil || !response.Success {
		t.Fatalf("apply leader heartbeat response=%+v err=%v", response, err)
	}
	runtime.NotifyLeaderContact()
	if !runtime.HasAuthority() {
		t.Fatal("recent accepted controller heartbeat did not establish authority")
	}
	runtime.lastLeaderContact.Store(time.Now().Add(-time.Second).UnixNano())
	if runtime.HasAuthority() {
		t.Fatal("expired controller heartbeat still granted authority")
	}
}
