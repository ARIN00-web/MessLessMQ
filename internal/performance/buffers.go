package performance

import (
	"errors"
	"io"
	"net"
)

var ErrInvalidBufferPoolConfig = errors.New("buffer pool sizes must be positive and max capacity must cover buffer size")

const defaultMaxIdleBuffers = 64

// BufferPool reuses short-lived byte buffers and retains at most 64 idle
// buffers. Unlike sync.Pool, its idle-buffer bound is deterministic. Buffers
// larger than maxRetain are discarded rather than pinning large allocations.
type BufferPool struct {
	free       chan []byte
	bufferSize int
	maxRetain  int
}

func NewBufferPool(bufferSize, maxRetain int) (*BufferPool, error) {
	if bufferSize <= 0 || maxRetain < bufferSize {
		return nil, ErrInvalidBufferPoolConfig
	}
	return &BufferPool{
		free:       make(chan []byte, defaultMaxIdleBuffers),
		bufferSize: bufferSize,
		maxRetain:  maxRetain,
	}, nil
}

func (p *BufferPool) Get() []byte {
	if p == nil {
		return nil
	}
	select {
	case buf := <-p.free:
		if cap(buf) >= p.bufferSize {
			return buf[:0]
		}
	default:
	}
	return make([]byte, 0, p.bufferSize)
}

func (p *BufferPool) Put(buf []byte) {
	if p == nil || cap(buf) < p.bufferSize || cap(buf) > p.maxRetain {
		return
	}
	select {
	case p.free <- buf[:0]:
	default: // The bounded idle pool is full; let this buffer be collected.
	}
}

// WriteBuffers writes byte slices in order without first concatenating them
// into another allocation. net.Buffers uses vectored I/O where the destination
// supports it and falls back to ordinary writes elsewhere. This avoids an
// application-side join/copy; it is not a guarantee of kernel-level zero-copy.
func WriteBuffers(w io.Writer, chunks ...[]byte) (int64, error) {
	buffers := make(net.Buffers, 0, len(chunks))
	for _, chunk := range chunks {
		if len(chunk) > 0 {
			buffers = append(buffers, chunk)
		}
	}
	return buffers.WriteTo(w)
}
