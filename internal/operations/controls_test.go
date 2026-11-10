package operations

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPauseResumeAndDrain(t *testing.T) {
	controls := NewControls()
	if err := controls.Pause(OperationProduce); err != nil {
		t.Fatal(err)
	}
	if _, err := controls.Begin(OperationProduce); !errors.Is(err, ErrPaused) {
		t.Fatalf("expected paused, got %v", err)
	}
	if err := controls.Resume(OperationProduce); err != nil {
		t.Fatal(err)
	}
	release, err := controls.Begin(OperationProduce)
	if err != nil {
		t.Fatal(err)
	}
	if got := controls.Snapshot().Active; got != 1 {
		t.Fatalf("active=%d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := controls.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected drain timeout, got %v", err)
	}
	if controls.Snapshot().State != StateDraining {
		t.Fatal("timed-out drain did not remain draining")
	}
	if _, err := controls.Begin(OperationConsume); !errors.Is(err, ErrDraining) {
		t.Fatalf("expected draining rejection, got %v", err)
	}
	release()
	release()
	if err := controls.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if controls.Snapshot().State != StateStopped {
		t.Fatalf("state=%s", controls.Snapshot().State)
	}
	if _, err := controls.Begin(OperationInternal); !errors.Is(err, ErrStopped) {
		t.Fatalf("expected stopped, got %v", err)
	}
}

func TestConcurrentDrainWaitsForInflightOperations(t *testing.T) {
	controls := NewControls()
	release, err := controls.Begin(OperationConsume)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- controls.Drain(context.Background()) }()
	select {
	case err := <-finished:
		t.Fatalf("drain returned before release: %v", err)
	case <-time.After(5 * time.Millisecond):
	}
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not finish")
	}
}
