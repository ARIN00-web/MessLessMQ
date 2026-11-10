package failure

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

func TestClassifyFailureKinds(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Class
	}{
		{name: "client eof", err: io.EOF, want: ClassClientDisconnected},
		{name: "connection reset", err: syscall.ECONNRESET, want: ClassClientDisconnected},
		{name: "deadline", err: context.DeadlineExceeded, want: ClassTimeout},
		{name: "canceled", err: context.Canceled, want: ClassCanceled},
		{name: "partial write", err: io.ErrShortWrite, want: ClassPartialWrite},
		{name: "record corruption", err: storage.ErrInvalidRecord, want: ClassCorruption},
		{name: "segment corruption", err: storage.ErrInvalidSegment, want: ClassCorruption},
		{name: "index corruption", err: storage.ErrCorruptIndex, want: ClassCorruption},
		{name: "closed file", err: os.ErrClosed, want: ClassClosed},
		{name: "unknown", err: errors.New("unexpected"), want: ClassUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.err); got != tt.want {
				t.Fatalf("Classify() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestMarkRetryable(t *testing.T) {
	cause := errors.New("temporary upstream failure")
	err := MarkRetryable(cause)

	if !IsRetryable(err) {
		t.Fatal("MarkRetryable() error was not identified as retryable")
	}
	if Classify(err) != ClassTransient {
		t.Fatalf("Classify() = %s, want %s", Classify(err), ClassTransient)
	}
	if !errors.Is(err, cause) {
		t.Fatal("retryable wrapper does not preserve the cause")
	}
}

type timeoutFailure struct{}

func (timeoutFailure) Error() string   { return "test timeout" }
func (timeoutFailure) Timeout() bool   { return true }
func (timeoutFailure) Temporary() bool { return true }

func TestClassifyNetworkTimeout(t *testing.T) {
	if got := Classify(timeoutFailure{}); got != ClassTimeout {
		t.Fatalf("Classify(timeoutFailure) = %s, want %s", got, ClassTimeout)
	}
}

func TestCorruptionAndPartialWriteAreNeverBlindlyRetried(t *testing.T) {
	for _, err := range []error{
		MarkRetryable(storage.ErrInvalidRecord),
		MarkRetryable(io.ErrShortWrite),
		MarkRetryable(os.ErrClosed),
	} {
		if IsRetryable(err) {
			t.Errorf("IsRetryable(%v) = true, want false", err)
		}
	}
}
