package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ARIN00-web/messlessmq/internal/config"
	"github.com/ARIN00-web/messlessmq/internal/observability"
	"github.com/ARIN00-web/messlessmq/internal/server"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("load broker configuration: %v", err)
	}
	bootstrapTopics, err := server.ParseBootstrapTopics(os.Getenv("MESSLESSMQ_BOOTSTRAP_TOPICS"))
	if err != nil {
		log.Fatalf("parse bootstrap topics: %v", err)
	}
	logger := observability.NewLogger(os.Stdout, observability.ParseLogLevel(cfg.LogLevel), "broker")
	runtime, err := server.New(server.Options{
		Config: cfg, BootstrapTopics: bootstrapTopics,
		AdminAddress: os.Getenv("MESSLESSMQ_ADMIN_ADDRESS"), Logger: logger,
	})
	if err != nil {
		log.Fatalf("initialize broker: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runtime.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("broker stopped with error", "error", err)
		os.Exit(1)
	}
}
