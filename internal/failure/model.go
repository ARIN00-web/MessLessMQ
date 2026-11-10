// Package failure classifies broker, storage, and client failures and provides
// explicit retry and recovery primitives.
//
// The package intentionally does not retry arbitrary errors automatically.
// Callers must mark an operation's error retryable only when repeating that
// operation is safe, for example when the operation uses an idempotency key.
package failure

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

// Class identifies the broad category of a failure.
type Class uint8

const (
	ClassUnknown Class = iota
	ClassClientDisconnected
	ClassTimeout
	ClassPartialWrite
	ClassCorruption
	ClassClosed
	ClassCanceled
	ClassTransient
)

func (c Class) String() string {
	switch c {
	case ClassClientDisconnected:
		return "client_disconnected"
	case ClassTimeout:
		return "timeout"
	case ClassPartialWrite:
		return "partial_write"
	case ClassCorruption:
		return "corruption"
	case ClassClosed:
		return "closed"
	case ClassCanceled:
		return "canceled"
	case ClassTransient:
		return "transient"
	default:
		return "unknown"
	}
}

// Classify maps common Go and MessLessMQ errors to a failure class.
// Unknown errors stay ClassUnknown; callers must not infer retry safety from
// an unknown error.
func Classify(err error) Class {
	if err == nil {
		return ClassUnknown
	}

	switch {
	case errors.Is(err, context.Canceled):
		return ClassCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return ClassTimeout
	case errors.Is(err, storage.ErrInvalidRecord),
		errors.Is(err, storage.ErrInvalidSegment),
		errors.Is(err, storage.ErrCorruptIndex):
		return ClassCorruption
	case errors.Is(err, io.ErrShortWrite):
		return ClassPartialWrite
	case errors.Is(err, os.ErrClosed):
		return ClassClosed
	case errors.Is(err, io.EOF),
		errors.Is(err, net.ErrClosed),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EPIPE):
		return ClassClientDisconnected
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ClassTimeout
	}

	var retryable *RetryableError
	if errors.As(err, &retryable) {
		return ClassTransient
	}

	return ClassUnknown
}

// RetryableError explicitly marks an operation failure as safe to retry.
// This is a policy marker, not a guarantee that the underlying operation is
// idempotent. The caller is responsible for making that determination.
type RetryableError struct {
	Cause error
}

func (e *RetryableError) Error() string {
	if e == nil || e.Cause == nil {
		return "retryable operation failure"
	}
	return "retryable operation failure: " + e.Cause.Error()
}

func (e *RetryableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// MarkRetryable explicitly marks err as safe for a bounded retry policy.
func MarkRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Cause: err}
}

// IsRetryable reports only explicitly marked retryable errors. Timeouts and
// disconnects are not retried automatically because a request may have been
// applied by the broker even when the acknowledgement was lost.
func IsRetryable(err error) bool {
	switch Classify(err) {
	case ClassCorruption, ClassPartialWrite, ClassClosed, ClassCanceled:
		return false
	}
	var retryable *RetryableError
	return errors.As(err, &retryable)
}
