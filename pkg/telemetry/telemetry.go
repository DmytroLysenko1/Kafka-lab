// Package telemetry is the tracing every service shares: one trace from the HTTP request that
// authorised a payment, through its outbox row and the topic, to the consumer that counted it.
// A service with no collector configured still propagates the context it is handed, so a trace
// is never broken by a hop that does not export.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var ErrExporter = errors.New("telemetry: cannot build the trace exporter")

// endpointVariable is the standard OpenTelemetry setting, read by the exporter itself; this
// package only looks at whether it is set.
const endpointVariable = "OTEL_EXPORTER_OTLP_ENDPOINT"

// FlushTimeout bounds how long a stopping service waits for its last spans to leave. It comes
// out of the same grace period as everything else a service does on SIGTERM — the consumer
// already spends up to 28 of the pod's 30 s (kafka/consumer.go) — so it is one second: a span
// that cannot be exported in a second is dropped, never paid for with a SIGKILL.
const FlushTimeout = time.Second

const instrumentation = "github.com/DmytroLysenko1/Kafka-lab"

// Setup makes W3C trace context the propagation format and, when a collector is configured,
// exports spans to it in batches. It returns the function that flushes and stops the exporter;
// without a collector that function does nothing.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv(endpointVariable) == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrExporter, err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

// Start opens a span for one unit of work and returns the function that ends it with how the
// work came out, so a failed use case shows as failed in the trace without each caller
// repeating the three calls that say so.
func Start(ctx context.Context, name string) (context.Context, func(error)) {
	ctx, span := otel.Tracer(instrumentation).Start(ctx, name)
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, name+" failed")
		}
		span.End()
	}
}

// TraceID is the trace the context belongs to, as a caller would search for it, or "" when it
// belongs to none.
func TraceID(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.HasTraceID() {
		return ""
	}
	return spanContext.TraceID().String()
}
