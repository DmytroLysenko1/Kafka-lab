package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// LogHandler adds the trace and span a record was logged in, so a log line found by its error
// leads to the whole trace, and a trace leads to every line written while it ran. Only records
// logged with a context carry them: that is why every log call in this repo is a *Context call.
func LogHandler(next slog.Handler) slog.Handler {
	return &traceHandler{
		next: next,
	}
}

type traceHandler struct {
	next slog.Handler
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

//nolint:gocritic // slog.Handler takes the record by value; the signature is the interface's, not this type's.
func (h *traceHandler) Handle(ctx context.Context, record slog.Record) error {
	if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", spanContext.TraceID().String()),
			slog.String("span_id", spanContext.SpanID().String()),
		)
	}
	return h.next.Handle(ctx, record)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{
		next: h.next.WithAttrs(attrs),
	}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{
		next: h.next.WithGroup(name),
	}
}
