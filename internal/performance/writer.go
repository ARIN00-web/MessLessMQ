package performance

import (
	"bufio"
	"errors"
	"io"
	"sync"
)

var (
	ErrWriterClosed    = errors.New("buffered sequential writer is closed")
	ErrSyncUnsupported = errors.New("underlying writer does not support Sync")
)

type syncer interface {
	Sync() error
}

// BufferedSequentialWriter batches small writes into a sequential stream.
// It serializes concurrent callers; Flush establishes a visibility boundary,
// while FlushAndSync additionally asks a durable sink (such as *os.File) to
// persist its buffers. A successful Flush alone is not a durability guarantee.
type BufferedSequentialWriter struct {
	mu     sync.Mutex
	writer *bufio.Writer
	sink   io.Writer
	closed bool
}

func NewBufferedSequentialWriter(sink io.Writer, bufferSize int) (*BufferedSequentialWriter, error) {
	if sink == nil || bufferSize <= 0 {
		return nil, errors.New("writer and positive buffer size are required")
	}
	return &BufferedSequentialWriter{
		writer: bufio.NewWriterSize(sink, bufferSize),
		sink:   sink,
	}, nil
}

func (w *BufferedSequentialWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrWriterClosed
	}
	return w.writer.Write(p)
}

func (w *BufferedSequentialWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWriterClosed
	}
	return w.writer.Flush()
}

func (w *BufferedSequentialWriter) FlushAndSync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWriterClosed
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	s, ok := w.sink.(syncer)
	if !ok {
		return ErrSyncUnsupported
	}
	return s.Sync()
}

// Close flushes remaining bytes once. It does not close the underlying sink;
// ownership of that resource remains with the caller.
func (w *BufferedSequentialWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (w *BufferedSequentialWriter) Buffered() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Buffered()
}
