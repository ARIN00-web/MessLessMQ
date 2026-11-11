package membership

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureIdentityCreatesAndReopensStableIdentity(t *testing.T) {
	dataDir := t.TempDir()
	if err := EnsureIdentity(dataDir, "orders-cluster", "broker-1"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureIdentity(dataDir, "orders-cluster", "broker-1"); err != nil {
		t.Fatalf("reopen same identity: %v", err)
	}
	info, err := os.Stat(filepath.Join(dataDir, "metadata", "identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("identity file permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestEnsureIdentityRejectsClusterOrBrokerChange(t *testing.T) {
	dataDir := t.TempDir()
	if err := EnsureIdentity(dataDir, "orders-cluster", "broker-1"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureIdentity(dataDir, "payments-cluster", "broker-1"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("cluster change error = %v, want ErrIdentityMismatch", err)
	}
	if err := EnsureIdentity(dataDir, "orders-cluster", "broker-2"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("broker change error = %v, want ErrIdentityMismatch", err)
	}
}

func TestEnsureIdentityRejectsMalformedIdentity(t *testing.T) {
	dataDir := t.TempDir()
	path := filepath.Join(dataDir, "metadata", "identity.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := EnsureIdentity(dataDir, "orders-cluster", "broker-1"); err == nil {
		t.Fatal("malformed identity file accepted")
	}
}
