//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

// The relay publishes a row from a process of its own, after the request that wrote it has
// answered; the row is the only thing that crosses that gap. It must carry the trace it was
// written in, and the claim must hand it back, or the trace ends at the database.
func TestAnEventCarriesTheTraceItWasWrittenInToTheRelay(t *testing.T) {
	storage, _ := freshStorage(t)
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	const requestsTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	request := otel.GetTextMapPropagator().Extract(t.Context(), propagation.MapCarrier{"traceparent": "00-" + requestsTrace + "-00f067aa0ba902b7-01"})

	err := storage.WithinTx(request, func(ctx context.Context) error {
		_, _, err := postgres.NewPaymentStore(storage).CreateOrGet(ctx, authorize(t, 1999), key(t, "traced"))
		return err
	})
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	var claimed []outbox.Record
	err = storage.WithinTx(t.Context(), func(ctx context.Context) error {
		var err error
		claimed, err = postgres.NewOutboxStore(storage).Claim(ctx, 10)
		return err
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d records, want 1", len(claimed))
	}
	relayed := otel.GetTextMapPropagator().Extract(t.Context(), propagation.MapCarrier(claimed[0].TraceContext))
	if diff := cmp.Diff(requestsTrace, telemetry.TraceID(relayed)); diff != "" {
		t.Errorf("the trace the relay will publish in (-want +got):\n%s", diff)
	}
}
