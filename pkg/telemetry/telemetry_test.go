package telemetry_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

// recording installs a provider that keeps every finished span, for the length of one test.
func recording(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return recorder
}

var errDatabase = errors.New("the database is not answering")

// A unit of work that failed is a failed span, with the error on it: a trace that showed every
// step green while the request answered 503 would send the person reading it the wrong way.
func TestASpanEndsWithHowTheWorkCameOut(t *testing.T) {
	type want struct {
		Name   string
		Status codes.Code
		Events int
	}
	tests := []struct {
		name string
		args error
		want want
	}{
		{name: "the work succeeded", args: nil, want: want{Name: "payments.Authorize", Status: codes.Unset}},
		{name: "the work failed", args: errDatabase, want: want{Name: "payments.Authorize", Status: codes.Error, Events: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := recording(t)

			_, end := telemetry.Start(t.Context(), "payments.Authorize")
			end(tt.args)

			ended := recorder.Ended()
			if len(ended) != 1 {
				t.Fatalf("ended spans = %d, want 1", len(ended))
			}
			got := want{Name: ended[0].Name(), Status: ended[0].Status().Code, Events: len(ended[0].Events())}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("span (-want +got):\n%s", diff)
			}
		})
	}
}

// A log line written inside a span names it, so the line found by its error leads to the whole
// trace; a line written outside any span says nothing about one rather than an empty id.
func TestALogLineNamesTheTraceItWasWrittenIn(t *testing.T) {
	recording(t)
	traced, end := telemetry.Start(t.Context(), "consumer.handle")
	defer end(nil)
	tests := []struct {
		name string
		args bool
		want string
	}{
		{name: "inside a span", args: true, want: telemetry.TraceID(traced)},
		{name: "outside any span", args: false, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var written bytes.Buffer
			logger := slog.New(telemetry.LogHandler(slog.NewJSONHandler(&written, nil))).With("service", "payments consumer")
			ctx := t.Context()
			if tt.args {
				ctx = traced
			}

			logger.InfoContext(ctx, "record handled")

			var line struct {
				TraceID string `json:"trace_id"`
				Service string `json:"service"`
			}
			if err := json.Unmarshal(written.Bytes(), &line); err != nil {
				t.Fatalf("decode %q: %v", written.String(), err)
			}
			if diff := cmp.Diff(tt.want, line.TraceID); diff != "" {
				t.Errorf("trace id (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff("payments consumer", line.Service); diff != "" {
				t.Errorf("attributes added before the handler must survive (-want +got):\n%s", diff)
			}
		})
	}
	if telemetry.TraceID(traced) == "" {
		t.Error("a context inside a span has no trace id")
	}
}

// Without a collector a service still runs and still propagates: Setup installs no exporter
// and hands back a shutdown that has nothing to flush.
func TestWithoutACollectorSetupExportsNothingAndShutsDownAtOnce(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	shutdown, err := telemetry.Setup(t.Context(), "payments api")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := shutdown(t.Context()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}
