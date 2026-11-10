package failure

import (
	"context"
	"errors"
	"testing"
	"time"
)

func noDelayPolicy(maxAttempts int) RetryPolicy {
	return RetryPolicy{
		MaxAttempts:  maxAttempts,
		InitialDelay: 0,
		MaxDelay:     0,
		Multiplier:   2,
	}
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	policy := noDelayPolicy(4)
	attempts := 0

	count, err := policy.Do(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return MarkRetryable(errors.New("temporary"))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	if count != 3 || attempts != 3 {
		t.Fatalf("attempts = (%d, %d), want (3, 3)", count, attempts)
	}
}

func TestRetryDoesNotRetryUnmarkedErrors(t *testing.T) {
	policy := noDelayPolicy(5)
	permanent := errors.New("invalid request")
	attempts := 0

	count, err := policy.Do(context.Background(), func(context.Context) error {
		attempts++
		return permanent
	})
	if !errors.Is(err, permanent) {
		t.Fatalf("Do() error = %v, want original error", err)
	}
	if count != 1 || attempts != 1 {
		t.Fatalf("attempts = (%d, %d), want (1, 1)", count, attempts)
	}
}

func TestRetryReturnsExhaustedError(t *testing.T) {
	policy := noDelayPolicy(3)
	cause := errors.New("upstream unavailable")

	count, err := policy.Do(context.Background(), func(context.Context) error {
		return MarkRetryable(cause)
	})
	if !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("Do() error = %v, want ErrRetryExhausted", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("Do() error does not preserve last cause: %v", err)
	}
	if count != 3 {
		t.Fatalf("attempt count = %d, want 3", count)
	}
}

func TestRetryStopsWhenContextIsCanceledDuringBackoff(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: time.Hour,
		MaxDelay:     time.Hour,
		Multiplier:   2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0

	count, err := policy.Do(ctx, func(context.Context) error {
		attempts++
		cancel()
		return MarkRetryable(errors.New("retry me"))
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() error = %v, want context.Canceled", err)
	}
	if count != 1 || attempts != 1 {
		t.Fatalf("attempts = (%d, %d), want (1, 1)", count, attempts)
	}
}

func TestRetryRejectsInvalidPolicyAndNilOperation(t *testing.T) {
	if _, err := (RetryPolicy{}).Do(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrInvalidRetryPolicy) {
		t.Fatalf("invalid policy error = %v", err)
	}

	if _, err := noDelayPolicy(1).Do(context.Background(), nil); !errors.Is(err, ErrNilOperation) {
		t.Fatalf("nil operation error = %v", err)
	}
}

func TestRetryDelayCapsAtMaximum(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     25 * time.Millisecond,
		Multiplier:   2,
	}

	if got := policy.delay(1); got != 10*time.Millisecond {
		t.Fatalf("delay(1) = %s", got)
	}
	if got := policy.delay(2); got != 20*time.Millisecond {
		t.Fatalf("delay(2) = %s", got)
	}
	if got := policy.delay(3); got != 25*time.Millisecond {
		t.Fatalf("delay(3) = %s, want capped delay", got)
	}
}
