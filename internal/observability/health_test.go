package observability

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthReportAndReadiness(t *testing.T) {
	h := NewHealthChecker()
	if err := h.Register("storage", time.Second, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := h.Register("replication", time.Second, func(context.Context) error { return errors.New("follower unavailable") }); err != nil {
		t.Fatal(err)
	}
	if err := h.Register("replication", time.Second, func(context.Context) error { return nil }); err == nil {
		t.Fatal("duplicate check accepted")
	}
	report := h.Report(context.Background())
	if report.Status != "degraded" || len(report.Checks) != 2 {
		t.Fatalf("unexpected report: %+v", report)
	}
	res := httptest.NewRecorder()
	h.ReadinessHandler().ServeHTTP(res, httptest.NewRequest("GET", "/health/ready", nil))
	if res.Code != 503 {
		t.Fatalf("readiness status=%d", res.Code)
	}
}

func TestHealthCheckTimeoutAndLiveness(t *testing.T) {
	h := NewHealthChecker()
	if err := h.Register("timeout", 5*time.Millisecond, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	report := h.Report(context.Background())
	if report.Status != "degraded" || report.Checks[0].Status != "failed" {
		t.Fatalf("timeout not reflected: %+v", report)
	}
	res := httptest.NewRecorder()
	LivenessHandler().ServeHTTP(res, httptest.NewRequest("GET", "/health/live", nil))
	if res.Code != 200 {
		t.Fatalf("liveness status=%d", res.Code)
	}
}

func TestDiagnosticsSnapshot(t *testing.T) {
	d := NewDiagnostics()
	snapshot := d.Snapshot()
	if snapshot.Goroutines < 1 || snapshot.NumCPU < 1 || snapshot.GoVersion == "" {
		t.Fatalf("incomplete diagnostics: %+v", snapshot)
	}
}
