package membership

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const identityVersion = 1

var ErrIdentityMismatch = errors.New("membership: persisted broker identity does not match configuration")

type persistedIdentity struct {
	Version   uint16 `json:"version"`
	ClusterID string `json:"cluster_id"`
	BrokerID  string `json:"broker_id"`
}

// EnsureIdentity pins the cluster and broker identity to a data directory.
// Network addresses are intentionally excluded because advertised addresses
// may change across deployments while the broker's durable identity must not.
// The file is atomically published and fsynced before the broker opens logs.
func EnsureIdentity(dataDir, clusterID, brokerID string) error {
	if strings.TrimSpace(dataDir) == "" || strings.TrimSpace(clusterID) == "" || strings.TrimSpace(brokerID) == "" {
		return ErrInvalidConfig
	}
	path := filepath.Join(dataDir, "metadata", "identity.json")
	data, err := os.ReadFile(path)
	if err == nil {
		var current persistedIdentity
		if err := json.Unmarshal(data, &current); err != nil {
			return fmt.Errorf("decode persisted broker identity: %w", err)
		}
		if current.Version != identityVersion || current.ClusterID != clusterID || current.BrokerID != brokerID {
			return fmt.Errorf("data directory %q has cluster=%q broker=%q; configured cluster=%q broker=%q: %w",
				dataDir, current.ClusterID, current.BrokerID, clusterID, brokerID, ErrIdentityMismatch)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read persisted broker identity: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create broker identity directory: %w", err)
	}
	encoded, err := json.MarshalIndent(persistedIdentity{Version: identityVersion, ClusterID: clusterID, BrokerID: brokerID}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode broker identity: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".identity-*.tmp")
	if err != nil {
		return fmt.Errorf("create broker identity temporary file: %w", err)
	}
	tempPath := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(tempPath)
	}
	if err := file.Chmod(0600); err != nil {
		cleanup()
		return err
	}
	if _, err := file.Write(encoded); err != nil {
		cleanup()
		return fmt.Errorf("write broker identity: %w", err)
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync broker identity: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close broker identity: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("publish broker identity: %w", err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open broker identity directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync broker identity directory: %w", err)
	}
	return nil
}
