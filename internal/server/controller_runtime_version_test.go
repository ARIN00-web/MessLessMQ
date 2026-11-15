package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/config"
	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/membership"
)

func TestProposeAtVersionRejectsStaleMetadataBeforeAppending(t *testing.T) {
	node, err := controller.OpenDurableNode("broker-1", []string{"broker-2", "broker-3"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	members, err := membership.NewRegistry("test-cluster", membership.Broker{
		ID: "broker-1", ClientAddress: "127.0.0.1:9092", PeerAddress: "127.0.0.1:19093",
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.BrokerID = "broker-1"
	runtime, err := newControllerRuntime("broker-1", []string{"broker-1", "broker-2", "broker-3"}, node, members, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.ProposeAtVersion(context.Background(), controller.Command{Type: controller.CommandNoop}, 1)
	if !errors.Is(err, controller.ErrMetadataConflict) {
		t.Fatalf("ProposeAtVersion error = %v, want metadata conflict", err)
	}
	if got := node.Status().LastLogIndex; got != 0 {
		t.Fatalf("stale proposal appended log index %d, want no append", got)
	}
}
