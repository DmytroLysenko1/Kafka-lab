package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

const callersTrace = "4bf92f3577b34da6a3ce929d0e0e4736"

// tracing installs what telemetry.Setup does with a collector configured, for one test.
func tracing(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	return recorder
}

// tracedChanger remembers the trace its context belonged to.
type tracedChanger struct {
	trace string
}

func (f *tracedChanger) Execute(ctx context.Context, cmd payments.ChangeCommand) (payments.ChangeResult, error) {
	f.trace = telemetry.TraceID(ctx)
	return payments.ChangeResult{PaymentID: cmd.PaymentID, Status: "captured"}, nil
}

// A caller that sends its trace has the request join it: the use case runs inside it, the
// span is named by the route pattern rather than the path, and the answer names the trace back — the
// one id that finds this request's outbox row, its record on the topic and the consumer that
// counted it.
func TestARequestJoinsTheCallersTraceAndAnswersWithIt(t *testing.T) {
	recorder := tracing(t)
	capture := &tracedChanger{}
	server := changeServerFor(t, capture)
	request := changeRequest(t, paymentPath+"/capture", apiKey)
	request.Header.Set("traceparent", "00-"+callersTrace+"-00f067aa0ba902b7-01")
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	type seen struct {
		Status     int
		Answered   string
		UseCaseRan string
		SpanNames  []string
	}
	names := []string{}
	for _, span := range recorder.Ended() {
		names = append(names, span.Name())
	}
	want := seen{Status: http.StatusOK, Answered: callersTrace, UseCaseRan: callersTrace, SpanNames: []string{"POST /merchants/{merchantID}/payments/{paymentID}/capture"}}
	got := seen{Status: response.Code, Answered: response.Header().Get("X-Trace-Id"), UseCaseRan: capture.trace, SpanNames: names}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("trace (-want +got):\n%s", diff)
	}
}
