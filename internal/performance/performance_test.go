package performance

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestBatchBuilderRespectsCountAndByteLimits(t *testing.T) {
	builder, err := NewBatchBuilder(2, 5)
	if err != nil {
		t.Fatal(err)
	}
	first := []byte("abc")
	if batch, err := builder.Add(first); err != nil || batch != nil {
		t.Fatalf("first Add = (%v, %v), want (nil, nil)", batch, err)
	}
	first[0] = 'X'
	if batch, err := builder.Add([]byte("de")); err != nil || batch != nil {
		t.Fatalf("second Add = (%v, %v), want (nil, nil)", batch, err)
	}
	if batch, err := builder.Add([]byte("f")); err != nil {
		t.Fatalf("third Add error = %v", err)
	} else if batch == nil || len(batch.Messages) != 2 || batch.Bytes != 5 {
		t.Fatalf("emitted batch = %#v, want 2 messages / 5 bytes", batch)
	} else if string(batch.Messages[0]) != "abc" {
		t.Fatalf("builder did not copy input: %q", batch.Messages[0])
	}
	remaining := builder.Flush()
	if remaining == nil || len(remaining.Messages) != 1 || string(remaining.Messages[0]) != "f" {
		t.Fatalf("Flush = %#v, want one message f", remaining)
	}
	if builder.Flush() != nil || builder.Len() != 0 || builder.Bytes() != 0 {
		t.Fatal("Flush did not reset builder")
	}
}

func TestBatchBuilderRejectsInvalidLimitsAndOversizedMessage(t *testing.T) {
	if _, err := NewBatchBuilder(0, 10); !errors.Is(err, ErrInvalidBatchLimits) {
		t.Fatalf("invalid limits error = %v", err)
	}
	builder, _ := NewBatchBuilder(2, 3)
	if _, err := builder.Add([]byte("four")); !errors.Is(err, ErrBatchMessageTooLarge) {
		t.Fatalf("oversized message error = %v", err)
	}
}

func TestBufferPoolReusesAndDropsOversizedBuffers(t *testing.T) {
	pool, err := NewBufferPool(64, 128)
	if err != nil {
		t.Fatal(err)
	}
	buf := pool.Get()
	if cap(buf) < 64 || len(buf) != 0 {
		t.Fatalf("Get returned len=%d cap=%d", len(buf), cap(buf))
	}
	buf = append(buf, "payload"...)
	pool.Put(buf)
	reused := pool.Get()
	if len(reused) != 0 || cap(reused) != cap(buf) {
		t.Fatalf("reused buffer len=%d cap=%d, want len=0 cap=%d", len(reused), cap(reused), cap(buf))
	}
	pool.Put(reused)
	large := make([]byte, 0, 256)
	pool.Put(large)
	if _, err := NewBufferPool(128, 64); !errors.Is(err, ErrInvalidBufferPoolConfig) {
		t.Fatalf("invalid pool config error = %v", err)
	}
}

func TestWriteBuffersPreservesOrderAndSkipsEmptyChunks(t *testing.T) {
	var dst bytes.Buffer
	n, err := WriteBuffers(&dst, []byte("one"), nil, []byte("-"), []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(dst.Len()) || dst.String() != "one-two" {
		t.Fatalf("WriteBuffers wrote %d bytes %q", n, dst.String())
	}
}

func TestBufferedSequentialWriterFlushAndClose(t *testing.T) {
	var dst bytes.Buffer
	writer, err := NewBufferedSequentialWriter(&dst, 32)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != 0 {
		t.Fatal("data escaped buffer before flush")
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if dst.String() != "alpha" {
		t.Fatalf("flushed data = %q", dst.String())
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("late")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("Write after Close error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestBufferedSequentialWriterSyncRequiresSyncableSink(t *testing.T) {
	var dst bytes.Buffer
	writer, _ := NewBufferedSequentialWriter(&dst, 16)
	if _, err := writer.Write([]byte("durable?")); err != nil {
		t.Fatal(err)
	}
	if err := writer.FlushAndSync(); !errors.Is(err, ErrSyncUnsupported) {
		t.Fatalf("FlushAndSync error = %v", err)
	}
}

type syncBuffer struct {
	bytes.Buffer
	synced bool
}

func (s *syncBuffer) Sync() error { s.synced = true; return nil }

func TestBufferedSequentialWriterFlushAndSync(t *testing.T) {
	sink := &syncBuffer{}
	writer, _ := NewBufferedSequentialWriter(sink, 16)
	_, _ = writer.Write([]byte("persist"))
	if err := writer.FlushAndSync(); err != nil {
		t.Fatal(err)
	}
	if !sink.synced || sink.String() != "persist" {
		t.Fatalf("sync=%v data=%q", sink.synced, sink.String())
	}
}

func TestBoundedQueueFIFOAndDrainAfterClose(t *testing.T) {
	queue, err := NewBoundedQueue[int](2)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.TrySubmit(10); err != nil {
		t.Fatal(err)
	}
	if err := queue.Submit(context.Background(), 20); err != nil {
		t.Fatal(err)
	}
	if err := queue.TrySubmit(30); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue error = %v", err)
	}
	queue.Close()
	for _, want := range []int{10, 20} {
		got, err := queue.Receive(context.Background())
		if err != nil || got != want {
			t.Fatalf("Receive = (%d, %v), want %d", got, err, want)
		}
	}
	if _, err := queue.Receive(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("Receive after drain = %v", err)
	}
	if err := queue.Submit(context.Background(), 40); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("Submit after close = %v", err)
	}
	stats := queue.Stats()
	if stats.Accepted != 2 || stats.Rejected != 1 || stats.Depth != 0 || !stats.Closed {
		t.Fatalf("queue stats = %+v", stats)
	}
}

func TestBoundedQueueSubmitBlocksUntilCapacityAvailable(t *testing.T) {
	queue, _ := NewBoundedQueue[int](1)
	if err := queue.Submit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- queue.Submit(ctx, 2) }()
	time.Sleep(10 * time.Millisecond)
	first, err := queue.Receive(context.Background())
	if err != nil || first != 1 {
		t.Fatalf("Receive = %d, %v", first, err)
	}
	if err := <-done; err != nil {
		t.Fatalf("blocked Submit = %v", err)
	}
	second, err := queue.Receive(context.Background())
	if err != nil || second != 2 {
		t.Fatalf("second Receive = %d, %v", second, err)
	}
	stats := queue.Stats()
	if stats.BlockedSubmissions == 0 || stats.BlockedTime <= 0 {
		t.Fatalf("expected blocked submission metrics, got %+v", stats)
	}
}

func TestBoundedQueueCloseWakesBlockedSubmitterAndReceiver(t *testing.T) {
	full, _ := NewBoundedQueue[int](1)
	if err := full.Submit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	submitDone := make(chan error, 1)
	go func() { submitDone <- full.Submit(context.Background(), 2) }()
	time.Sleep(10 * time.Millisecond)
	full.Close()
	if err := <-submitDone; !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("blocked Submit after Close = %v", err)
	}

	empty, _ := NewBoundedQueue[int](1)
	receiveDone := make(chan error, 1)
	go func() {
		_, err := empty.Receive(context.Background())
		receiveDone <- err
	}()
	time.Sleep(10 * time.Millisecond)
	empty.Close()
	if err := <-receiveDone; !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("blocked Receive after Close = %v", err)
	}
}

func TestBoundedQueueCancellationAndConcurrentUse(t *testing.T) {
	queue, _ := NewBoundedQueue[int](1)
	_ = queue.Submit(context.Background(), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := queue.Submit(ctx, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit cancellation = %v", err)
	}
	_, _ = queue.Receive(context.Background())

	var wg sync.WaitGroup
	for producer := 0; producer < 4; producer++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := queue.Submit(context.Background(), base+i); err != nil {
					return
				}
			}
		}(producer * 100)
	}
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for i := 0; i < 400; i++ {
			if _, err := queue.Receive(context.Background()); err != nil {
				return
			}
		}
	}()
	wg.Wait()
	<-consumerDone
}

func TestLatencyRecorderHistogramAndPercentile(t *testing.T) {
	recorder, err := NewLatencyRecorder([]time.Duration{20 * time.Millisecond, time.Millisecond, 10 * time.Millisecond, time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	recorder.Observe(500*time.Microsecond, false)
	recorder.Observe(5*time.Millisecond, false)
	recorder.Observe(15*time.Millisecond, true)
	snapshot := recorder.Snapshot()
	if snapshot.Count != 3 || snapshot.Errors != 1 || snapshot.Min != 500*time.Microsecond || snapshot.Max != 15*time.Millisecond {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	p50, err := recorder.Percentile(0.50)
	if err != nil || p50 != 10*time.Millisecond {
		t.Fatalf("p50 = %v, %v", p50, err)
	}
	p99, err := recorder.Percentile(0.99)
	if err != nil || p99 != 20*time.Millisecond {
		t.Fatalf("p99 = %v, %v", p99, err)
	}
	if got := snapshot.ThroughputPerSecond(time.Second); got != 3 {
		t.Fatalf("throughput = %v", got)
	}
	if _, err := recorder.Percentile(1.1); !errors.Is(err, ErrInvalidPercentile) {
		t.Fatalf("bad percentile error = %v", err)
	}
}

func TestLatencyRecorderConcurrent(t *testing.T) {
	recorder, _ := NewLatencyRecorder([]time.Duration{time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				recorder.Observe(time.Microsecond, false)
			}
		}()
	}
	wg.Wait()
	if got := recorder.Snapshot().Count; got != 800 {
		t.Fatalf("count = %d", got)
	}
}

func BenchmarkBatchBuilder(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 1024)
	for _, batchSize := range []int{1, 16, 64, 256} {
		b.Run("messages="+itoa(batchSize), func(b *testing.B) {
			builder, _ := NewBatchBuilder(batchSize, batchSize*1024)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for i := 0; i < b.N; i++ {
				if _, err := builder.Add(payload); err != nil {
					b.Fatal(err)
				}
				if builder.Len() == batchSize {
					_ = builder.Flush()
				}
			}
		})
	}
}

func BenchmarkBufferedSequentialWriter(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 256)
	writer, _ := NewBufferedSequentialWriter(io.Discard, 32*1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writer.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := writer.Flush(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDirectSequentialWrites(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 256)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		if _, err := io.Discard.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBufferPool(b *testing.B) {
	pool, _ := NewBufferPool(4096, 64*1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf = append(buf, "payload"...)
		pool.Put(buf)
	}
}

func BenchmarkWriteBuffers(b *testing.B) {
	parts := [][]byte{bytes.Repeat([]byte("a"), 1024), bytes.Repeat([]byte("b"), 1024), bytes.Repeat([]byte("c"), 1024)}
	b.ReportAllocs()
	b.SetBytes(3072)
	for i := 0; i < b.N; i++ {
		if _, err := WriteBuffers(io.Discard, parts...); err != nil {
			b.Fatal(err)
		}
	}
}

func itoa(n int) string {
	if n == 1 {
		return "1"
	}
	if n == 16 {
		return "16"
	}
	if n == 64 {
		return "64"
	}
	return "256"
}
