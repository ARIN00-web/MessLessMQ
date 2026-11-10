package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestStructuredLoggingRedactsSecret(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, slog.LevelInfo, "broker")
	logger.Info("request", "api_key", Secret("do-not-print"), "request_id", "req-1")
	if strings.Contains(output.String(), "do-not-print") || !strings.Contains(output.String(), "[REDACTED]") || !strings.Contains(output.String(), "broker") {
		t.Fatalf("unexpected log: %s", output.String())
	}
}

func TestRequestIDContextAndLogLevel(t *testing.T) {
	ctx := WithRequestID(context.Background(), "abc")
	if got := RequestIDFromContext(ctx); got != "abc" {
		t.Fatalf("request ID=%q", got)
	}
	if ParseLogLevel("DEBUG") != slog.LevelDebug || ParseLogLevel("nonsense") != slog.LevelInfo {
		t.Fatal("unexpected log level parsing")
	}
}
