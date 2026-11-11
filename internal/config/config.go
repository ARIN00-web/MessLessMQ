package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	BrokerID             string
	ListenAddress        string
	AdvertiseAddress     string
	DataDir              string
	LogLevel             string
	MaxConnections       int
	RequestTimeout       time.Duration
	ShutdownTimeout      time.Duration
	TLSCertFile          string
	TLSKeyFile           string
	TLSClientCAFile      string
	TLSClientCertFile    string
	TLSClientKeyFile     string
	RequireClientCert    bool
	RateLimitPerSecond   float64
	RateLimitBurst       int
	ClusterID            string
	PeerListenAddress    string
	PeerAdvertiseAddress string
	SeedPeers            []string
	PeerJoinTimeout      time.Duration
	ReplicaAssignments   string
	MinISR               int
	ControllerVoters     []string
	ElectionTimeoutMin   time.Duration
	ElectionTimeoutMax   time.Duration
	HeartbeatInterval    time.Duration
}

func Defaults() Config {
	return Config{
		BrokerID: "broker-1", ListenAddress: "127.0.0.1:9092",
		AdvertiseAddress: "127.0.0.1:9092", DataDir: "./data", LogLevel: "info",
		MaxConnections: 10000, RequestTimeout: 10 * time.Second,
		ShutdownTimeout: 30 * time.Second, RateLimitPerSecond: 1000,
		RateLimitBurst: 2000, PeerJoinTimeout: 30 * time.Second, MinISR: 1,
		ElectionTimeoutMin: 900 * time.Millisecond, ElectionTimeoutMax: 1500 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond,
	}
}

type LookupEnv func(string) (string, bool)

func LoadFromEnv() (Config, error) { return Load(os.LookupEnv) }

func Load(getenv LookupEnv) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("environment lookup is nil")
	}
	cfg := Defaults()
	var err error
	readString := func(key string, target *string) {
		if value, ok := getenv("MESSLESSMQ_" + key); ok {
			*target = strings.TrimSpace(value)
		}
	}
	readString("BROKER_ID", &cfg.BrokerID)
	readString("LISTEN_ADDRESS", &cfg.ListenAddress)
	if value, ok := getenv("MESSLESSMQ_ADVERTISE_ADDRESS"); ok {
		cfg.AdvertiseAddress = strings.TrimSpace(value)
	} else if _, listenOverridden := getenv("MESSLESSMQ_LISTEN_ADDRESS"); listenOverridden {
		cfg.AdvertiseAddress = cfg.ListenAddress
	}
	readString("DATA_DIR", &cfg.DataDir)
	readString("LOG_LEVEL", &cfg.LogLevel)
	readString("TLS_CERT_FILE", &cfg.TLSCertFile)
	readString("TLS_KEY_FILE", &cfg.TLSKeyFile)
	readString("TLS_CLIENT_CA_FILE", &cfg.TLSClientCAFile)
	readString("TLS_CLIENT_CERT_FILE", &cfg.TLSClientCertFile)
	readString("TLS_CLIENT_KEY_FILE", &cfg.TLSClientKeyFile)
	readString("CLUSTER_ID", &cfg.ClusterID)
	readString("PEER_LISTEN_ADDRESS", &cfg.PeerListenAddress)
	readString("PEER_ADVERTISE_ADDRESS", &cfg.PeerAdvertiseAddress)
	readString("REPLICA_ASSIGNMENTS", &cfg.ReplicaAssignments)
	if value, ok := getenv("MESSLESSMQ_CONTROLLER_VOTERS"); ok {
		for _, voter := range strings.Split(value, ",") {
			if voter = strings.TrimSpace(voter); voter != "" {
				cfg.ControllerVoters = append(cfg.ControllerVoters, voter)
			}
		}
	}
	if value, ok := getenv("MESSLESSMQ_SEEDS"); ok {
		for _, seed := range strings.Split(value, ",") {
			if seed = strings.TrimSpace(seed); seed != "" {
				cfg.SeedPeers = append(cfg.SeedPeers, seed)
			}
		}
	}
	if cfg.AdvertiseAddress == "" {
		cfg.AdvertiseAddress = cfg.ListenAddress
	}
	if cfg.MaxConnections, err = intValue(getenv, "MAX_CONNECTIONS", cfg.MaxConnections); err != nil {
		return Config{}, err
	}
	if cfg.RateLimitBurst, err = intValue(getenv, "RATE_LIMIT_BURST", cfg.RateLimitBurst); err != nil {
		return Config{}, err
	}
	if cfg.MinISR, err = intValue(getenv, "MIN_ISR", cfg.MinISR); err != nil {
		return Config{}, err
	}
	if cfg.RequestTimeout, err = durationValue(getenv, "REQUEST_TIMEOUT", cfg.RequestTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = durationValue(getenv, "SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.PeerJoinTimeout, err = durationValue(getenv, "PEER_JOIN_TIMEOUT", cfg.PeerJoinTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ElectionTimeoutMin, err = durationValue(getenv, "ELECTION_TIMEOUT_MIN", cfg.ElectionTimeoutMin); err != nil {
		return Config{}, err
	}
	if cfg.ElectionTimeoutMax, err = durationValue(getenv, "ELECTION_TIMEOUT_MAX", cfg.ElectionTimeoutMax); err != nil {
		return Config{}, err
	}
	if cfg.HeartbeatInterval, err = durationValue(getenv, "HEARTBEAT_INTERVAL", cfg.HeartbeatInterval); err != nil {
		return Config{}, err
	}
	if cfg.RateLimitPerSecond, err = floatValue(getenv, "RATE_LIMIT_PER_SECOND", cfg.RateLimitPerSecond); err != nil {
		return Config{}, err
	}
	if cfg.RequireClientCert, err = boolValue(getenv, "REQUIRE_CLIENT_CERT", cfg.RequireClientCert); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func intValue(getenv LookupEnv, key string, fallback int) (int, error) {
	value, ok := getenv("MESSLESSMQ_" + key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("MESSLESSMQ_%s: %w", key, err)
	}
	return parsed, nil
}

func floatValue(getenv LookupEnv, key string, fallback float64) (float64, error) {
	value, ok := getenv("MESSLESSMQ_" + key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return 0, fmt.Errorf("MESSLESSMQ_%s: %w", key, err)
	}
	return parsed, nil
}

func durationValue(getenv LookupEnv, key string, fallback time.Duration) (time.Duration, error) {
	value, ok := getenv("MESSLESSMQ_" + key)
	if !ok {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("MESSLESSMQ_%s: %w", key, err)
	}
	return parsed, nil
}

func boolValue(getenv LookupEnv, key string, fallback bool) (bool, error) {
	value, ok := getenv("MESSLESSMQ_" + key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("MESSLESSMQ_%s: %w", key, err)
	}
	return parsed, nil
}

func (c Config) Validate() error {
	var problems []string
	if strings.TrimSpace(c.BrokerID) == "" {
		problems = append(problems, "broker ID is required")
	}
	if err := validateAddress(c.ListenAddress); err != nil {
		problems = append(problems, "listen address: "+err.Error())
	}
	if err := validateAddress(c.AdvertiseAddress); err != nil {
		problems = append(problems, "advertise address: "+err.Error())
	}
	if strings.TrimSpace(c.DataDir) == "" {
		problems = append(problems, "data directory is required")
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "warning", "error":
	default:
		problems = append(problems, "log level must be debug, info, warn, or error")
	}
	if c.MaxConnections <= 0 {
		problems = append(problems, "max connections must be positive")
	}
	if c.RequestTimeout <= 0 {
		problems = append(problems, "request timeout must be positive")
	}
	if c.ShutdownTimeout <= 0 {
		problems = append(problems, "shutdown timeout must be positive")
	}
	if c.RateLimitPerSecond <= 0 {
		problems = append(problems, "rate limit per second must be positive")
	}
	if c.RateLimitBurst <= 0 {
		problems = append(problems, "rate limit burst must be positive")
	}
	if c.MinISR < 1 {
		problems = append(problems, "minimum in-sync replicas must be at least one")
	}
	peerConfigured := c.ClusterID != "" || c.PeerListenAddress != "" || c.PeerAdvertiseAddress != "" || len(c.SeedPeers) > 0
	if len(c.ControllerVoters) > 0 {
		if !peerConfigured {
			problems = append(problems, "controller voters require cluster membership configuration")
		}
		seenVoters := make(map[string]struct{}, len(c.ControllerVoters))
		selfFound := false
		for _, voter := range c.ControllerVoters {
			voter = strings.TrimSpace(voter)
			if voter == "" || len(voter) > 128 {
				problems = append(problems, "controller voter IDs must be non-empty and at most 128 characters")
				continue
			}
			if _, exists := seenVoters[voter]; exists {
				problems = append(problems, fmt.Sprintf("duplicate controller voter %q", voter))
				continue
			}
			seenVoters[voter] = struct{}{}
			if voter == c.BrokerID {
				selfFound = true
			}
		}
		if !selfFound {
			problems = append(problems, "controller voter list must include this broker ID")
		}
		if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
			problems = append(problems, "controller election timeout max must exceed a positive min")
		}
		if c.HeartbeatInterval <= 0 || c.HeartbeatInterval*2 >= c.ElectionTimeoutMin {
			problems = append(problems, "controller heartbeat interval must be positive and less than half the minimum election timeout")
		}
	}
	if strings.TrimSpace(c.ReplicaAssignments) != "" && !peerConfigured {
		problems = append(problems, "replica assignments require cluster membership configuration")
	}
	if peerConfigured {
		if strings.TrimSpace(c.ClusterID) == "" || len(c.ClusterID) > 128 {
			problems = append(problems, "cluster ID is required and must be at most 128 characters")
		}
		if err := validateAddress(c.PeerListenAddress); err != nil {
			problems = append(problems, "peer listen address: "+err.Error())
		}
		if err := validateAdvertisedAddress(c.PeerAdvertiseAddress); err != nil {
			problems = append(problems, "peer advertise address: "+err.Error())
		}
		if c.PeerJoinTimeout <= 0 {
			problems = append(problems, "peer join timeout must be positive")
		}
		seenSeeds := make(map[string]struct{}, len(c.SeedPeers))
		for _, seed := range c.SeedPeers {
			id, address, ok := strings.Cut(strings.TrimSpace(seed), "@")
			id, address = strings.TrimSpace(id), strings.TrimSpace(address)
			if !ok || id == "" || address == "" {
				problems = append(problems, fmt.Sprintf("invalid seed %q: expected broker-id@host:port", seed))
				continue
			}
			if _, exists := seenSeeds[id]; exists {
				problems = append(problems, fmt.Sprintf("duplicate seed broker %q", id))
				continue
			}
			seenSeeds[id] = struct{}{}
			if err := validateAdvertisedAddress(address); err != nil {
				problems = append(problems, fmt.Sprintf("seed %q address: %s", id, err))
				continue
			}
			// A bootstrap broker may list itself as its only seed. This is useful
			// for ordinal-based deployments where every pod receives the same
			// seed configuration: the first pod ignores itself while later pods
			// join it. A self seed must still use this broker's advertised peer
			// address so a typo cannot silently turn into a false bootstrap.
			if id == c.BrokerID && address != c.PeerAdvertiseAddress {
				problems = append(problems, "self seed address must match peer advertise address")
			}
		}
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		problems = append(problems, "TLS certificate and key must be configured together")
	}
	if (c.TLSClientCertFile == "") != (c.TLSClientKeyFile == "") {
		problems = append(problems, "TLS client certificate and key must be configured together")
	}
	if c.TLSClientCAFile != "" && c.TLSCertFile == "" {
		problems = append(problems, "client CA requires TLS server certificate and key")
	}
	if c.RequireClientCert && (c.TLSCertFile == "" || c.TLSClientCAFile == "") {
		problems = append(problems, "mutual TLS requires server certificate, key, and client CA")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func validateAddress(address string) error {
	if strings.TrimSpace(address) == "" {
		return errors.New("must not be empty")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	_ = host
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	return nil
}

func validateAdvertisedAddress(address string) error {
	if err := validateAddress(address); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if strings.TrimSpace(host) == "" {
		return errors.New("advertised host must not be empty")
	}
	return nil
}
