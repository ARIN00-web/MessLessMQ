package observability

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

type Secret string

func (Secret) LogValue() slog.Value { return slog.StringValue("[REDACTED]") }

func ParseLogLevel(value string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func NewLogger(w io.Writer, level slog.Level, component string) *slog.Logger {
	if w == nil {
		w = io.Discard
	}
	logger := slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
	if component != "" {
		logger = logger.With("component", component)
	}
	return logger
}

type requestIDKey struct{}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, requestID)
}

func RequestIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}
