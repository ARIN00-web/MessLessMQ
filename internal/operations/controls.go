package operations

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrDraining         = errors.New("broker is draining and is not accepting new operations")
	ErrStopped          = errors.New("broker is stopped")
	ErrPaused           = errors.New("operation is paused by an operational control")
	ErrUnknownOperation = errors.New("unknown operation")
)

type State string

const (
	StateRunning  State = "running"
	StateDraining State = "draining"
	StateStopped  State = "stopped"
)

type Operation string

const (
	OperationProduce  Operation = "produce"
	OperationConsume  Operation = "consume"
	OperationAdmin    Operation = "admin"
	OperationInternal Operation = "internal"
)

type Snapshot struct {
	State  State              `json:"state"`
	Active int                `json:"active_operations"`
	Paused map[Operation]bool `json:"paused"`
}

type Controls struct {
	mu     sync.Mutex
	state  State
	active int
	idle   chan struct{}
	paused map[Operation]bool
}

func NewControls() *Controls {
	idle := make(chan struct{})
	close(idle)
	return &Controls{state: StateRunning, idle: idle, paused: make(map[Operation]bool)}
}

func validOperation(op Operation) bool {
	switch op {
	case OperationProduce, OperationConsume, OperationAdmin, OperationInternal:
		return true
	default:
		return false
	}
}

func (c *Controls) Pause(op Operation) error {
	if !validOperation(op) {
		return ErrUnknownOperation
	}
	c.mu.Lock()
	c.paused[op] = true
	c.mu.Unlock()
	return nil
}

func (c *Controls) Resume(op Operation) error {
	if !validOperation(op) {
		return ErrUnknownOperation
	}
	c.mu.Lock()
	delete(c.paused, op)
	c.mu.Unlock()
	return nil
}

// Begin registers an in-flight operation and returns an idempotent release
// function. New operations are rejected while paused or draining.
func (c *Controls) Begin(op Operation) (func(), error) {
	if c == nil || !validOperation(op) {
		return nil, ErrUnknownOperation
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case StateDraining:
		return nil, ErrDraining
	case StateStopped:
		return nil, ErrStopped
	}
	if c.paused[op] {
		return nil, ErrPaused
	}
	if c.active == 0 {
		c.idle = make(chan struct{})
	}
	c.active++
	var once sync.Once
	release := func() {
		once.Do(func() {
			c.mu.Lock()
			c.active--
			if c.active == 0 {
				close(c.idle)
			}
			c.mu.Unlock()
		})
	}
	return release, nil
}

func (c *Controls) BeginDrain() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.state == StateRunning {
		c.state = StateDraining
	}
	c.mu.Unlock()
}

func (c *Controls) Drain(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.BeginDrain()
	c.mu.Lock()
	idle := c.idle
	c.mu.Unlock()
	select {
	case <-idle:
		c.mu.Lock()
		if c.active == 0 && c.state == StateDraining {
			c.state = StateStopped
		}
		c.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Controls) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{State: StateStopped}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	paused := make(map[Operation]bool, len(c.paused))
	for op, value := range c.paused {
		paused[op] = value
	}
	return Snapshot{State: c.state, Active: c.active, Paused: paused}
}
