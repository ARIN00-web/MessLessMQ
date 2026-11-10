package failure

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	ErrInvalidRetryPolicy = errors.New("invalid retry policy")
	ErrNilOperation       = errors.New("retry operation is nil")
	ErrRetryExhausted     = errors.New("retry attempts exhausted")
)

// RetryPolicy defines bounded exponential-backoff behavior.
// Jitter, when supplied, transforms the calculated delay. It should return a
// non-negative duration no greater than MaxDelay.
type RetryPolicy struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       func(time.Duration) time.Duration
}

// DefaultRetryPolicy returns a conservative bounded retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:  4,
		InitialDelay: 50 * time.Millisecond,
		MaxDelay:     time.Second,
		Multiplier:   2,
	}
}

// RetryError reports an exhausted retry operation while preserving the last
// error for errors.Is/errors.As inspection.
type RetryError struct {
	Attempts int
	Cause    error
}

func (e *RetryError) Error() string {
	return fmt.Sprintf("%v after %d attempts: %v", ErrRetryExhausted, e.Attempts, e.Cause)
}

func (e *RetryError) Unwrap() error { return e.Cause }

func (e *RetryError) Is(target error) bool { return target == ErrRetryExhausted }

// Do invokes operation until it succeeds, returns a non-retryable error, the
// attempt limit is reached, or ctx is canceled. It returns the number of
// attempts performed.
//
// An operation is retried only when its returned error is wrapped with
// MarkRetryable. This prevents blind retries of writes whose outcome may be
// ambiguous after a client disconnect or acknowledgement timeout.
func (p RetryPolicy) Do(
	ctx context.Context,
	operation func(context.Context) error,
) (int, error) {
	if operation == nil {
		return 0, ErrNilOperation
	}
	if err := p.validate(); err != nil {
		return 0, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var lastErr error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return attempt - 1, err
		}

		err := operation(ctx)
		if err == nil {
			return attempt, nil
		}
		lastErr = err

		if !IsRetryable(err) {
			return attempt, err
		}
		if attempt == p.MaxAttempts {
			return attempt, &RetryError{Attempts: attempt, Cause: err}
		}

		delay := p.delay(attempt)
		if err := waitContext(ctx, delay); err != nil {
			return attempt, errors.Join(err, lastErr)
		}
	}

	return p.MaxAttempts, &RetryError{Attempts: p.MaxAttempts, Cause: lastErr}
}

func (p RetryPolicy) validate() error {
	if p.MaxAttempts <= 0 ||
		p.InitialDelay < 0 ||
		p.MaxDelay < p.InitialDelay ||
		p.Multiplier < 1 ||
		math.IsNaN(p.Multiplier) ||
		math.IsInf(p.Multiplier, 0) {
		return ErrInvalidRetryPolicy
	}
	return nil
}

func (p RetryPolicy) delay(failedAttempt int) time.Duration {
	delay := p.InitialDelay
	for i := 1; i < failedAttempt; i++ {
		if delay >= p.MaxDelay {
			delay = p.MaxDelay
			break
		}
		next := float64(delay) * p.Multiplier
		if next >= float64(p.MaxDelay) {
			delay = p.MaxDelay
			break
		}
		delay = time.Duration(next)
	}

	if p.Jitter != nil {
		delay = p.Jitter(delay)
		if delay < 0 {
			delay = 0
		}
		if delay > p.MaxDelay {
			delay = p.MaxDelay
		}
	}
	return delay
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
