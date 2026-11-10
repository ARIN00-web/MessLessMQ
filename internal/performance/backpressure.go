package performance

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrQueueClosed = errors.New("bounded queue is closed")
	ErrQueueFull   = errors.New("bounded queue is full")
)

// QueueStats reports queue admission behaviour. BlockedSubmissions counts
// Submit calls that had to wait at least once; Rejected counts TrySubmit calls
// rejected because the queue was full.
type QueueStats struct {
	Capacity           int
	Depth              int
	Accepted           uint64
	Rejected           uint64
	BlockedSubmissions uint64
	BlockedTime        time.Duration
	Closed             bool
}

// BoundedQueue is a fixed-capacity, concurrency-safe FIFO. Submit applies
// backpressure by waiting for capacity or context cancellation. TrySubmit is
// the explicit load-shedding alternative. Close wakes blocked senders/receivers
// and allows already-queued items to be drained before Receive returns closed.
type BoundedQueue[T any] struct {
	mu       sync.Mutex
	items    []T
	head     int
	tail     int
	count    int
	closed   bool
	notEmpty chan struct{}
	notFull  chan struct{}

	accepted  atomic.Uint64
	rejected  atomic.Uint64
	blocked   atomic.Uint64
	waitNanos atomic.Int64
}

func NewBoundedQueue[T any](capacity int) (*BoundedQueue[T], error) {
	if capacity <= 0 {
		return nil, errors.New("queue capacity must be positive")
	}
	return &BoundedQueue[T]{
		items:    make([]T, capacity),
		notEmpty: make(chan struct{}),
		notFull:  make(chan struct{}),
	}, nil
}

func signal(ch *chan struct{}) {
	close(*ch)
	*ch = make(chan struct{})
}

func (q *BoundedQueue[T]) Submit(ctx context.Context, item T) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	started := time.Now()
	waited := false
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			if waited {
				q.blocked.Add(1)
				q.waitNanos.Add(time.Since(started).Nanoseconds())
			}
			return ErrQueueClosed
		}
		if q.count < len(q.items) {
			q.items[q.tail] = item
			q.tail = (q.tail + 1) % len(q.items)
			q.count++
			q.accepted.Add(1)
			if q.count == 1 {
				signal(&q.notEmpty)
			}
			q.mu.Unlock()
			if waited {
				q.blocked.Add(1)
				q.waitNanos.Add(time.Since(started).Nanoseconds())
			}
			return nil
		}
		waitCh := q.notFull
		q.mu.Unlock()
		waited = true
		select {
		case <-ctx.Done():
			q.blocked.Add(1)
			q.waitNanos.Add(time.Since(started).Nanoseconds())
			return ctx.Err()
		case <-waitCh:
		}
	}
}

func (q *BoundedQueue[T]) TrySubmit(item T) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	if q.count == len(q.items) {
		q.rejected.Add(1)
		return ErrQueueFull
	}
	q.items[q.tail] = item
	q.tail = (q.tail + 1) % len(q.items)
	q.count++
	q.accepted.Add(1)
	if q.count == 1 {
		signal(&q.notEmpty)
	}
	return nil
}

func (q *BoundedQueue[T]) Receive(ctx context.Context) (T, error) {
	var zero T
	if ctx == nil {
		return zero, errors.New("context must not be nil")
	}
	for {
		q.mu.Lock()
		if q.count > 0 {
			item := q.items[q.head]
			q.items[q.head] = zero
			q.head = (q.head + 1) % len(q.items)
			q.count--
			if q.count == len(q.items)-1 {
				signal(&q.notFull)
			}
			q.mu.Unlock()
			return item, nil
		}
		if q.closed {
			q.mu.Unlock()
			return zero, ErrQueueClosed
		}
		waitCh := q.notEmpty
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-waitCh:
		}
	}
}

func (q *BoundedQueue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	signal(&q.notEmpty)
	signal(&q.notFull)
}

func (q *BoundedQueue[T]) Stats() QueueStats {
	q.mu.Lock()
	stats := QueueStats{
		Capacity: len(q.items),
		Depth:    q.count,
		Closed:   q.closed,
	}
	q.mu.Unlock()
	stats.Accepted = q.accepted.Load()
	stats.Rejected = q.rejected.Load()
	stats.BlockedSubmissions = q.blocked.Load()
	stats.BlockedTime = time.Duration(q.waitNanos.Load())
	return stats
}
