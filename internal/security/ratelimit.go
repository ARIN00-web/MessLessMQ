package security

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrRateLimiterCapacity = errors.New("rate limiter key capacity reached")
	ErrInvalidRateLimit    = errors.New("rate and burst must be positive")
)

type bucket struct {
	tokens float64
	last   time.Time
}

type RateLimiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	maxKeys int
	buckets map[string]bucket
	now     func() time.Time
}

func NewRateLimiter(rate float64, burst, maxKeys int) (*RateLimiter, error) {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || burst <= 0 || maxKeys <= 0 {
		return nil, ErrInvalidRateLimit
	}
	return &RateLimiter{rate: rate, burst: float64(burst), maxKeys: maxKeys, buckets: make(map[string]bucket), now: time.Now}, nil
}

// Allow consumes one token. A false result includes the approximate wait until
// one token is available. Keys are bounded; callers should Forget keys when
// principals or connections are retired.
func (l *RateLimiter) Allow(key string) (allowed bool, retryAfter time.Duration, err error) {
	if l == nil || strings.TrimSpace(key) == "" {
		return false, 0, errors.New("rate limiter key is required")
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, exists := l.buckets[key]
	if !exists {
		if len(l.buckets) >= l.maxKeys {
			return false, 0, ErrRateLimiterCapacity
		}
		b = bucket{tokens: l.burst, last: now}
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		l.buckets[key] = b
		return true, 0, nil
	}
	l.buckets[key] = b
	wait := time.Duration(math.Ceil((1 - b.tokens) / l.rate * float64(time.Second)))
	return false, wait, nil
}

func (l *RateLimiter) Forget(key string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	delete(l.buckets, key)
	l.mu.Unlock()
}

func (l *RateLimiter) KeyCount() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// RateLimitMiddleware applies a token bucket after authentication when wrapped
// inside the authentication middleware. keyFunc should return a stable,
// authenticated principal or connection identity, not an untrusted header.
func RateLimitMiddleware(limiter *RateLimiter, keyFunc func(*http.Request) string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if limiter == nil || keyFunc == nil {
			http.Error(w, "rate limiting unavailable", http.StatusServiceUnavailable)
			return
		}
		allowed, wait, err := limiter.Allow(keyFunc(r))
		if err != nil {
			http.Error(w, "rate limiting unavailable", http.StatusServiceUnavailable)
			return
		}
		if !allowed {
			seconds := int64(math.Ceil(wait.Seconds()))
			if seconds < 1 {
				seconds = 1
			}
			w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
