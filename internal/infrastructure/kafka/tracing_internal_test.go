package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/outboxwire"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

const (
	requestsTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	traceparent   = "00-" + requestsTrace + "-00f067aa0ba902b7-01"
)

// traced installs what telemetry.Setup does with a collector configured, for one test.
func traced(t *testing.T) *sdktrace.TracerProvider {
	t.Helper()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(tracetest.NewSpanRecorder()))
	previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	return provider
}

// The relay publishes a row long after the request that raised it answered, from a sweep of its
// own. The record it publishes belongs to the request's trace — the one the row stored — not to
// the sweep; a row with no trace stays in the sweep's.
func TestARecordIsPublishedInTheTraceItsRowWasWrittenIn(t *testing.T) {
	traced(t)
	sweep, end := telemetry.Start(t.Context(), "outbox.Sweep")
	defer end(nil)
	tests := []struct {
		name string
		args map[string]string
		want string
	}{
		{name: "a row written inside a request", args: map[string]string{"traceparent": traceparent}, want: requestsTrace},
		{name: "a row written before rows carried a trace", args: nil, want: telemetry.TraceID(sweep)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(outboxwire.Payload{PaymentID: "p-1", MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR", OccurredAt: time.Now()})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			published, err := publisherWithSchema().message(sweep, &outbox.Record{ID: 41, AggregateID: "p-1", EventType: outboxwire.EventTypeAuthorized, Payload: payload, TraceContext: tt.args})
			if err != nil {
				t.Fatalf("message: %v", err)
			}

			// The record's own context is what is under test: it is the parent kotel publishes under.
			if diff := cmp.Diff(tt.want, telemetry.TraceID(published.Context)); diff != "" { //nolint:contextcheck // see above
				t.Errorf("trace (-want +got):\n%s", diff)
			}
		})
	}
}

// A record is handled in the trace it arrived in, and still under the consumer's own deadline
// and cancellation: kotel's process context alone is built on context.Background, and handling
// a record in it would outlive the consumer's stop.
func TestARecordIsHandledInItsTraceUnderTheConsumersDeadline(t *testing.T) {
	provider := traced(t)
	tracer := kotel.NewTracer(kotel.TracerProvider(provider), kotel.TracerPropagator(propagation.TraceContext{}))
	arrived := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier{"traceparent": traceparent})
	record := &kgo.Record{Topic: "payments.main", Context: arrived}
	consumer, stop := context.WithCancel(t.Context())

	handling, span := processing(consumer, tracer, record)
	defer span.End()
	stop()

	if diff := cmp.Diff(requestsTrace, telemetry.TraceID(handling)); diff != "" {
		t.Errorf("trace (-want +got):\n%s", diff)
	}
	if !errors.Is(handling.Err(), context.Canceled) {
		t.Errorf("handling context err = %v after the consumer stopped, want %v", handling.Err(), context.Canceled)
	}
}
