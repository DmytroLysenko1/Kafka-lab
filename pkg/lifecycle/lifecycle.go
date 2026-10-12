// Package lifecycle is the part every service's main has in common: a JSON logger that names
// the service and the trace on every line, tracing, a context that ends on SIGINT or SIGTERM,
// one line when the service stops and why, and an exit code that says whether it stopped on
// purpose.
package lifecycle

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

// Run is a service's own work. It returns when the context ends, or with the reason it
// could not go on.
type Run func(ctx context.Context, logger *slog.Logger) error

// Main runs a service and exits with 1 if it failed. os.Exit skips deferred calls, so it is
// called here, after run has returned and released everything it held. The name is the
// service's as traces and logs carry it: one word, hyphenated.
func Main(name string, run Run) {
	if code := serve(name, run); code != 0 {
		os.Exit(code)
	}
}

func serve(name string, run Run) int {
	logger := slog.New(telemetry.LogHandler(slog.NewJSONHandler(os.Stdout, nil))).With("service", name)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	flush, err := telemetry.Setup(ctx, name)
	if err != nil {
		logger.ErrorContext(ctx, "service not started", "error", err)
		return 1
	}
	defer flushTraces(ctx, logger, flush)

	if err := run(ctx, logger); err != nil {
		logger.ErrorContext(ctx, "service stopped", "error", err)
		return 1
	}
	logger.InfoContext(ctx, "service stopped")
	return 0
}

// flushTraces sends the last spans on a context of its own: the service's context is already
// cancelled by the signal that stopped it, and the spans of that very shutdown are the ones an
// operator wants to see.
func flushTraces(ctx context.Context, logger *slog.Logger, flush func(context.Context) error) {
	flushing, cancel := context.WithTimeout(context.WithoutCancel(ctx), telemetry.FlushTimeout)
	defer cancel()
	if err := flush(flushing); err != nil {
		logger.WarnContext(ctx, "traces not flushed", "error", err)
	}
}
