package observability

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"time"
)

type HealthCheck func(context.Context) error

type checkEntry struct {
	name    string
	timeout time.Duration
	check   HealthCheck
}

type HealthChecker struct {
	mu     sync.RWMutex
	checks map[string]checkEntry
}

type CheckResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type HealthReport struct {
	Status      string        `json:"status"`
	GeneratedAt time.Time     `json:"generated_at"`
	Checks      []CheckResult `json:"checks"`
}

func NewHealthChecker() *HealthChecker {
	return &HealthChecker{checks: make(map[string]checkEntry)}
}

func (h *HealthChecker) Register(name string, timeout time.Duration, check HealthCheck) error {
	if h == nil || name == "" || check == nil || timeout <= 0 {
		return errors.New("health check requires a name, function, and positive timeout")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.checks[name]; exists {
		return errors.New("health check already registered")
	}
	h.checks[name] = checkEntry{name: name, timeout: timeout, check: check}
	return nil
}

func (h *HealthChecker) Report(ctx context.Context) HealthReport {
	report := HealthReport{Status: "ok", GeneratedAt: time.Now().UTC()}
	if h == nil {
		report.Status = "degraded"
		return report
	}
	h.mu.RLock()
	entries := make([]checkEntry, 0, len(h.checks))
	for _, entry := range h.checks {
		entries = append(entries, entry)
	}
	h.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	for _, entry := range entries {
		checkCtx, cancel := context.WithTimeout(ctx, entry.timeout)
		err := entry.check(checkCtx)
		cancel()
		result := CheckResult{Name: entry.name, Status: "ok"}
		if err != nil {
			report.Status = "degraded"
			result.Status = "failed"
			result.Error = err.Error()
		}
		report.Checks = append(report.Checks, result)
	}
	return report
}

func LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("live\n"))
	})
}

func (h *HealthChecker) ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		report := h.Report(r.Context())
		w.Header().Set("Content-Type", "application/json")
		if report.Status != "ok" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(report)
	})
}

type Diagnostics struct{ startedAt time.Time }

type DiagnosticSnapshot struct {
	Uptime         time.Duration `json:"uptime"`
	Goroutines     int           `json:"goroutines"`
	HeapAllocBytes uint64        `json:"heap_alloc_bytes"`
	HeapSysBytes   uint64        `json:"heap_sys_bytes"`
	HeapObjects    uint64        `json:"heap_objects"`
	GCCycles       uint32        `json:"gc_cycles"`
	NumCPU         int           `json:"num_cpu"`
	GoVersion      string        `json:"go_version"`
}

func NewDiagnostics() *Diagnostics { return &Diagnostics{startedAt: time.Now()} }

func (d *Diagnostics) Snapshot() DiagnosticSnapshot {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	started := time.Now()
	if d != nil && !d.startedAt.IsZero() {
		started = d.startedAt
	}
	return DiagnosticSnapshot{
		Uptime: time.Since(started), Goroutines: runtime.NumGoroutine(),
		HeapAllocBytes: memory.HeapAlloc, HeapSysBytes: memory.HeapSys,
		HeapObjects: memory.HeapObjects, GCCycles: memory.NumGC,
		NumCPU: runtime.NumCPU(), GoVersion: runtime.Version(),
	}
}

func (d *Diagnostics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(d.Snapshot())
	})
}
