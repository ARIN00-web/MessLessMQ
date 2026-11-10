package observability

import (
	"math"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestMetricsRegistryAndPrometheusHandler(t *testing.T) {
	r := NewRegistry()
	requests, err := r.NewCounter("messlessmq_requests_total", "Total broker requests")
	if err != nil {
		t.Fatal(err)
	}
	requests.Inc()
	if err := requests.Add(2); err != nil {
		t.Fatal(err)
	}
	gauge, err := r.NewGauge("messlessmq_active_connections", "Current active connections")
	if err != nil {
		t.Fatal(err)
	}
	if err := gauge.Set(3); err != nil {
		t.Fatal(err)
	}
	histogram, err := r.NewHistogram("messlessmq_request_seconds", "Request duration", []float64{0.1, 0.5, 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []float64{0.05, 0.2, 0.7} {
		if err := histogram.Observe(value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.NewCounter("messlessmq_requests_total", "Duplicate"); err != ErrMetricExists {
		t.Fatalf("expected duplicate error, got %v", err)
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	res := httptest.NewRecorder()
	r.Handler().ServeHTTP(res, req)
	body := res.Body.String()
	for _, expected := range []string{
		"# TYPE messlessmq_requests_total counter", "messlessmq_requests_total 3",
		"messlessmq_active_connections 3", `messlessmq_request_seconds_bucket{le="0.1"} 1`,
		`messlessmq_request_seconds_bucket{le="+Inf"} 3`, "messlessmq_request_seconds_count 3",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics output missing %q:\n%s", expected, body)
		}
	}
}

func TestMetricsRejectInvalidDefinitionsAndValues(t *testing.T) {
	r := NewRegistry()
	if _, err := r.NewGauge("bad name", "bad"); err != ErrInvalidMetric {
		t.Fatalf("expected invalid metric, got %v", err)
	}
	if _, err := r.NewHistogram("h", "hist", []float64{1, 1}); err == nil {
		t.Fatal("expected unordered bucket error")
	}
	c, err := r.NewCounter("counter_total", "counter")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Add(-1); err == nil {
		t.Fatal("counter accepted negative increment")
	}
	if err := c.Add(math.NaN()); err == nil {
		t.Fatal("counter accepted NaN")
	}
	g, _ := r.NewGauge("gauge_value", "gauge")
	if err := g.Set(math.Inf(1)); err == nil {
		t.Fatal("gauge accepted infinity")
	}
}

func TestMetricsConcurrentUpdates(t *testing.T) {
	r := NewRegistry()
	counter, err := r.NewCounter("events_total", "events")
	if err != nil {
		t.Fatal(err)
	}
	const workers, perWorker = 8, 500
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				counter.Inc()
			}
		}()
	}
	wg.Wait()
	if got := r.Snapshot()[0].Value; got != workers*perWorker {
		t.Fatalf("counter=%v", got)
	}
}
