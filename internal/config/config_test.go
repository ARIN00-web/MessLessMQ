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
