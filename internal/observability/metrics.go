package observability

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrInvalidMetric = errors.New("invalid metric definition")
	ErrMetricExists  = errors.New("metric already registered")
)

type metricKind uint8

const (
	counterKind metricKind = iota + 1
	gaugeKind
	histogramKind
)

type metric struct {
	name    string
	help    string
	kind    metricKind
	value   float64
	buckets []float64
	counts  []uint64
	count   uint64
	sum     float64
}

type Registry struct {
	mu      sync.RWMutex
	metrics map[string]*metric
}

type Counter struct {
	m  *metric
	mu *sync.RWMutex
}

type Gauge struct {
	m  *metric
	mu *sync.RWMutex
}

type Histogram struct {
	m  *metric
	mu *sync.RWMutex
}

type MetricSnapshot struct {
	Name    string    `json:"name"`
	Help    string    `json:"help"`
	Type    string    `json:"type"`
	Value   float64   `json:"value,omitempty"`
	Buckets []float64 `json:"buckets,omitempty"`
	Counts  []uint64  `json:"counts,omitempty"`
	Count   uint64    `json:"count,omitempty"`
	Sum     float64   `json:"sum,omitempty"`
}

func NewRegistry() *Registry {
	return &Registry{metrics: make(map[string]*metric)}
}

func (r *Registry) register(name, help string, kind metricKind, buckets []float64) (*metric, error) {
	if r == nil || !validMetricName(name) || strings.TrimSpace(help) == "" {
		return nil, ErrInvalidMetric
	}
	if kind == histogramKind {
		if len(buckets) == 0 {
			return nil, fmt.Errorf("%w: histogram requires at least one bucket", ErrInvalidMetric)
		}
		for i, bound := range buckets {
			if math.IsNaN(bound) || math.IsInf(bound, 0) || (i > 0 && bound <= buckets[i-1]) {
				return nil, fmt.Errorf("%w: histogram buckets must be finite and strictly increasing", ErrInvalidMetric)
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.metrics[name]; exists {
		return nil, ErrMetricExists
	}
	m := &metric{name: name, help: help, kind: kind, buckets: append([]float64(nil), buckets...)}
	if kind == histogramKind {
		m.counts = make([]uint64, len(buckets))
	}
	r.metrics[name] = m
	return m, nil
}

func validMetricName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		valid := r == '_' || r == ':' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (i > 0 && r >= '0' && r <= '9')
		if !valid {
			return false
		}
	}
	return true
}

func (r *Registry) NewCounter(name, help string) (*Counter, error) {
	m, err := r.register(name, help, counterKind, nil)
	if err != nil {
		return nil, err
	}
	return &Counter{m: m, mu: &r.mu}, nil
}

func (r *Registry) NewGauge(name, help string) (*Gauge, error) {
	m, err := r.register(name, help, gaugeKind, nil)
	if err != nil {
		return nil, err
	}
	return &Gauge{m: m, mu: &r.mu}, nil
}

func (r *Registry) NewHistogram(name, help string, buckets []float64) (*Histogram, error) {
	m, err := r.register(name, help, histogramKind, buckets)
	if err != nil {
		return nil, err
	}
	return &Histogram{m: m, mu: &r.mu}, nil
}

func (c *Counter) Add(delta float64) error {
	if c == nil || c.m == nil || math.IsNaN(delta) || math.IsInf(delta, 0) || delta < 0 {
		return errors.New("counter increment must be finite and non-negative")
	}
	c.mu.Lock()
	c.m.value += delta
	c.mu.Unlock()
	return nil
}

func (c *Counter) Inc() { _ = c.Add(1) }

func (g *Gauge) Set(value float64) error {
	if g == nil || g.m == nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return errors.New("gauge value must be finite")
	}
	g.mu.Lock()
	g.m.value = value
	g.mu.Unlock()
	return nil
}

func (g *Gauge) Add(delta float64) error {
	if g == nil || g.m == nil || math.IsNaN(delta) || math.IsInf(delta, 0) {
		return errors.New("gauge delta must be finite")
	}
	g.mu.Lock()
	g.m.value += delta
	g.mu.Unlock()
	return nil
}

func (h *Histogram) Observe(value float64) error {
	if h == nil || h.m == nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return errors.New("histogram observation must be finite")
	}
	h.mu.Lock()
	for i, bound := range h.m.buckets {
		if value <= bound {
			h.m.counts[i]++
		}
	}
	h.m.count++
	h.m.sum += value
	h.mu.Unlock()
	return nil
}

func (r *Registry) Snapshot() []MetricSnapshot {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]MetricSnapshot, 0, len(r.metrics))
	for _, m := range r.metrics {
		s := MetricSnapshot{Name: m.name, Help: m.help}
		switch m.kind {
		case counterKind:
			s.Type, s.Value = "counter", m.value
		case gaugeKind:
			s.Type, s.Value = "gauge", m.value
		case histogramKind:
			s.Type = "histogram"
			s.Buckets = append([]float64(nil), m.buckets...)
			s.Counts = append([]uint64(nil), m.counts...)
			s.Count, s.Sum = m.count, m.sum
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *Registry) WritePrometheus(w io.Writer) error {
	if w == nil {
		return errors.New("metrics writer is nil")
	}
	for _, s := range r.Snapshot() {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", s.Name, escapeHelp(s.Help), s.Name, s.Type); err != nil {
			return err
		}
		switch s.Type {
		case "counter", "gauge":
			if _, err := fmt.Fprintf(w, "%s %s\n", s.Name, formatFloat(s.Value)); err != nil {
				return err
			}
		case "histogram":
			for i, bound := range s.Buckets {
				if _, err := fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", s.Name, formatFloat(bound), s.Counts[i]); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %s\n%s_count %d\n", s.Name, s.Count, s.Name, formatFloat(s.Sum), s.Name, s.Count); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := r.WritePrometheus(w); err != nil {
			http.Error(w, "unable to render metrics", http.StatusInternalServerError)
		}
	})
}

func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

func formatFloat(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
