package config

import (
	"testing"
	"time"
)

func TestLoadDefaultsAndEnvironmentOverrides(t *testing.T) {
	values := map[string]string{"MESSLESSMQ_BROKER_ID": "broker-9", "MESSLESSMQ_LISTEN_ADDRESS": "127.0.0.1:9192", "MESSLESSMQ_REQUEST_TIMEOUT": "3s"}
	cfg, err := Load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BrokerID != "broker-9" || cfg.ListenAddress != "127.0.0.1:9192" || cfg.AdvertiseAddress != "127.0.0.1:9192" || cfg.RequestTimeout != 3*time.Second {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestConfigValidationAndParsingErrors(t *testing.T) {
	cfg := Defaults()
	cfg.TLSCertFile = "server.crt"
	if err := cfg.Validate(); err == nil {
		t.Fatal("certificate without key accepted")
	}
	cfg = Defaults()
	cfg.TLSClientCertFile = "client.crt"
	if err := cfg.Validate(); err == nil {
		t.Fatal("client certificate without key accepted")
	}
	cfg = Defaults()
	cfg.ListenAddress = "localhost:99999"
	if err := cfg.Validate(); err == nil {
		t.Fatal("invalid port accepted")
	}
	_, err := Load(func(key string) (string, bool) {
		if key == "MESSLESSMQ_MAX_CONNECTIONS" {
			return "many", true
		}
		return "", false
	})
	if err == nil {
		t.Fatal("invalid integer environment value accepted")
	}
}

func TestValidMutualTLSConfig(t *testing.T) {
	cfg := Defaults()
	cfg.RequireClientCert = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("mutual TLS accepted without certificates")
	}
	cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile = "server.crt", "server.key", "clients.pem"
	cfg.TLSClientCertFile, cfg.TLSClientKeyFile = "client.crt", "client.key"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadClusterMembershipConfiguration(t *testing.T) {
	values := map[string]string{
		"MESSLESSMQ_BROKER_ID":              "broker-2",
		"MESSLESSMQ_CLUSTER_ID":             "orders-cluster",
		"MESSLESSMQ_PEER_LISTEN_ADDRESS":    ":19093",
		"MESSLESSMQ_PEER_ADVERTISE_ADDRESS": "broker-2:19093",
		"MESSLESSMQ_SEEDS":                  "broker-1@broker-1:19093,broker-3@broker-3:19093",
		"MESSLESSMQ_PEER_JOIN_TIMEOUT":      "45s",
	}
	cfg, err := Load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClusterID != "orders-cluster" || cfg.PeerListenAddress != ":19093" || cfg.PeerAdvertiseAddress != "broker-2:19093" || len(cfg.SeedPeers) != 2 || cfg.PeerJoinTimeout != 45*time.Second {
		t.Fatalf("unexpected cluster configuration: %+v", cfg)
	}
}

func TestClusterMembershipConfigurationMustBeComplete(t *testing.T) {
	cfg := Defaults()
	cfg.ClusterID = "orders-cluster"
	if err := cfg.Validate(); err == nil {
		t.Fatal("cluster ID without peer addresses should be rejected")
	}
	cfg = Defaults()
	cfg.BrokerID = "broker-1"
	cfg.ClusterID = "orders-cluster"
	cfg.PeerListenAddress = ":19093"
	cfg.PeerAdvertiseAddress = "broker-1:19093"
	cfg.SeedPeers = []string{"broker-1@broker-1:19093"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid bootstrap self seed should be allowed: %v", err)
	}
	cfg.SeedPeers = []string{"broker-1@broker-2:19093"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("self seed with a different peer address should be rejected")
	}
	cfg.SeedPeers = nil
	cfg.PeerAdvertiseAddress = ":19093"
	if err := cfg.Validate(); err == nil {
		t.Fatal("empty advertised peer host should be rejected")
	}
}

func TestLoadReplicationConfiguration(t *testing.T) {
	values := map[string]string{
		"MESSLESSMQ_BROKER_ID":              "broker-2",
		"MESSLESSMQ_CLUSTER_ID":             "orders-cluster",
		"MESSLESSMQ_PEER_LISTEN_ADDRESS":    ":19093",
		"MESSLESSMQ_PEER_ADVERTISE_ADDRESS": "broker-2:19093",
		"MESSLESSMQ_SEEDS":                  "broker-1@broker-1:19093",
		"MESSLESSMQ_REPLICA_ASSIGNMENTS":    "orders:0=broker-1,broker-2,broker-3",
		"MESSLESSMQ_MIN_ISR":                "2",
	}
	cfg, err := Load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReplicaAssignments != values["MESSLESSMQ_REPLICA_ASSIGNMENTS"] || cfg.MinISR != 2 {
		t.Fatalf("unexpected replication config: %+v", cfg)
	}
}

func TestReplicaAssignmentsRequireMembership(t *testing.T) {
	cfg := Defaults()
	cfg.ReplicaAssignments = "orders:0=broker-1,broker-2"
	if err := cfg.Validate(); err == nil {
		t.Fatal("replica assignments without cluster membership should be rejected")
	}
}

func TestLoadControllerVoterConfiguration(t *testing.T) {
	values := map[string]string{
		"MESSLESSMQ_BROKER_ID":              "broker-2",
		"MESSLESSMQ_CLUSTER_ID":             "orders-cluster",
		"MESSLESSMQ_PEER_LISTEN_ADDRESS":    ":19093",
		"MESSLESSMQ_PEER_ADVERTISE_ADDRESS": "broker-2:19093",
		"MESSLESSMQ_CONTROLLER_VOTERS":      "broker-1, broker-2,broker-3",
		"MESSLESSMQ_ELECTION_TIMEOUT_MIN":   "800ms",
		"MESSLESSMQ_ELECTION_TIMEOUT_MAX":   "1300ms",
		"MESSLESSMQ_HEARTBEAT_INTERVAL":     "150ms",
	}
	cfg, err := Load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.ControllerVoters) != 3 || cfg.ControllerVoters[1] != "broker-2" || cfg.ElectionTimeoutMin != 800*time.Millisecond || cfg.ElectionTimeoutMax != 1300*time.Millisecond || cfg.HeartbeatInterval != 150*time.Millisecond {
		t.Fatalf("unexpected controller config: %+v", cfg)
	}
}

func TestControllerVotersRequireFixedValidMembership(t *testing.T) {
	cfg := Defaults()
	cfg.BrokerID = "broker-1"
	cfg.ControllerVoters = []string{"broker-1", "broker-2", "broker-3"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("controller voters without membership should be rejected")
	}
	cfg.ClusterID = "orders-cluster"
	cfg.PeerListenAddress = ":19092"
	cfg.PeerAdvertiseAddress = "broker-1:19092"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid controller voter config rejected: %v", err)
	}
	cfg.ControllerVoters = []string{"broker-1", "broker-1"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("duplicate controller voters should be rejected")
	}
	cfg.ControllerVoters = []string{"broker-2", "broker-3"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("controller voter config without self should be rejected")
	}
}
