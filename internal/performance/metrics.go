package performance

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"
)

var ErrInvalidPercentile = errors.New("percentile must be between 0 and 1")

type HistogramBucket struct {
	UpperBound time.Duration
	Infinite   bool
	Count      uint64 // cumulative count at this boundary
}

type LatencySnapshot struct {
	Count   uint64
	Errors  uint64
	Total   time.Duration
	Min     time.Duration
	Max     time.Duration
	Buckets []HistogramBucket
}

// LatencyRecorder records count, total, min, max, failures, and a cumulative
// latency histogram. Bounds must be positive; they are sorted and deduplicated.
type LatencyRecorder struct {
	mu     sync.Mutex
	bounds []time.Duration
	counts []uint64
	count  uint64
	errors uint64
	total  time.Duration
	min    time.Duration
	max    time.Duration
}

func NewLatencyRecorder(bounds []time.Duration) (*LatencyRecorder, error) {
	if len(bounds) == 0 {
		return nil, errors.New("at least one latency bucket is required")
	}
	ordered := append([]time.Duration(nil), bounds...)
	for _, bound := range ordered {
		if bound <= 0 {
			return nil, errors.New("latency bucket bounds must be positive")
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	unique := ordered[:0]
	for _, bound := range ordered {
		if len(unique) == 0 || unique[len(unique)-1] != bound {
			unique = append(unique, bound)
		}
	}
	return &LatencyRecorder{bounds: unique, counts: make([]uint64, len(unique)+1)}, nil
}

func (r *LatencyRecorder) Observe(duration time.Duration, failed bool) {
	if duration < 0 {
		duration = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count == 0 || duration < r.min {
		r.min = duration
	}
	if duration > r.max {
		r.max = duration
	}
	r.count++
	if failed {
		r.errors++
	}
	r.total += duration
	index := sort.Search(len(r.bounds), func(i int) bool { return duration <= r.bounds[i] })
	r.counts[index]++
}

func (r *LatencyRecorder) Snapshot() LatencySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := LatencySnapshot{
		Count: r.count, Errors: r.errors, Total: r.total,
		Min: r.min, Max: r.max,
		Buckets: make([]HistogramBucket, len(r.bounds)+1),
	}
	var cumulative uint64
	for i, bound := range r.bounds {
		cumulative += r.counts[i]
		out.Buckets[i] = HistogramBucket{UpperBound: bound, Count: cumulative}
	}
	cumulative += r.counts[len(r.counts)-1]
	out.Buckets[len(out.Buckets)-1] = HistogramBucket{Infinite: true, Count: cumulative}
	return out
}

// Percentile returns the upper bound of the first histogram bucket containing
// the requested rank. It is an approximation limited by the configured bounds.
func (r *LatencyRecorder) Percentile(p float64) (time.Duration, error) {
	if math.IsNaN(p) || p < 0 || p > 1 {
		return 0, ErrInvalidPercentile
	}
	snapshot := r.Snapshot()
	if snapshot.Count == 0 {
		return 0, errors.New("no latency observations")
	}
	rank := uint64(math.Ceil(p * float64(snapshot.Count)))
	if rank == 0 {
		rank = 1
	}
	for _, bucket := range snapshot.Buckets {
		if bucket.Count >= rank {
			if bucket.Infinite {
				return snapshot.Max, nil
			}
			return bucket.UpperBound, nil
		}
	}
	return snapshot.Max, nil
}

func (s LatencySnapshot) ThroughputPerSecond(elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(s.Count) / elapsed.Seconds()
}
