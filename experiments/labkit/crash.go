package labkit

import (
	"log/slog"
	"os"
	"syscall"
)

// Crash kills this process the way a real crash would. SIGKILL cannot be caught, so no
// deferred close runs and nothing buffered is flushed — which is the point, and why the
// kill is delivered to the process rather than returned from main.
func Crash(msg string, attrs ...any) {
	slog.Warn(msg, attrs...)
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		os.Exit(137)
	}
	select {} // unreachable: SIGKILL is delivered before the next statement runs
}
