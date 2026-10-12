package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracing follows a payment's trace across the topic: a producer writes the trace a record
// belongs to into its traceparent header, and a consumer continues it. Every client of this
// package takes the hooks, so a record moved to a retry tier or the dead letter topic stays in
// the trace of the payment it carries.
func tracing() (*kotel.Tracer, kgo.Opt) {
	tracer := kotel.NewTracer()
	return tracer, kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(tracer)).Hooks()...)
}

// processing is the context a record is handled in: the caller's, for its deadline and its
// cancellation, carrying the span of the record's own trace. kotel's process context alone is
// built on context.Background and would let handling outlive a stop.
func processing(ctx context.Context, tracer *kotel.Tracer, record *kgo.Record) (context.Context, trace.Span) {
	_, span := tracer.WithProcessSpan(record)
	return trace.ContextWithSpan(ctx, span), span
}

// raisedIn is the trace an outbox row was written in, continued under ctx: the record's publish
// span becomes a child of the request that raised the event rather than of the sweep that
// happened to publish it. A row with no trace stays in the sweep's.
func raisedIn(ctx context.Context, traceContext map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(traceContext))
}
