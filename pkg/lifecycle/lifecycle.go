// Package lifecycle is the part every service's main has in common: a JSON logger, a
// context that ends on SIGINT or SIGTERM, one line when the service stops and why, and an
// exit code that says whether it stopped on purpose.
package lifecycle

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// Run is a service's own work. It returns when the context ends, or with the reason it
// could not go on.
type Run func(ctx context.Context, logger *slog.Logger) error

// Main runs a service and exits with 1 if it failed. os.Exit skips deferred calls, so it is
// called here, after run has returned and released everything it held.
func Main(name string, run Run) {
	if code := serve(name, run); code != 0 {
		os.Exit(code)
	}
}

func serve(name string, run Run) int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger); err != nil {
		logger.ErrorContext(ctx, name+" stopped", "error", err)
		return 1
	}
	logger.InfoContext(ctx, name+" stopped")
	return 0
}
