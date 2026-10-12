package kafka

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/outboxwire"
)

// publisherWithSchema is the publisher as NewPublisher leaves it, minus the registry and the
// broker: every message of the schema registered under one id at its index.
func publisherWithSchema() *Publisher {
	serde := sr.NewSerde()
	for index, event := range wireEvents() {
		serde.Register(schemaID, event.message(), sr.EncodeFn(encodeProto), sr.Index(index))
	}
	return &Publisher{serde: serde, topic: "payments.main"}
}

// What the relay writes, the consumer reads as the same movement: the two halves of the wire
// format are separate tables (publisher.go and decode.go), and a row whose type the publisher
// put at one index and the consumer read at another would move the wrong sum.
func TestEveryEventTypeIsReadBackAsTheMovementItWasPublishedAs(t *testing.T) {
	occurred := time.Date(2026, time.October, 5, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		args string
		want merchants.PaymentEvent
	}{
		{name: "authorized", args: outboxwire.EventTypeAuthorized, want: merchants.PaymentEvent{Kind: merchant.KindAuthorized, EventID: "41", MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR", OccurredAt: occurred}},
		{name: "captured", args: outboxwire.EventTypeCaptured, want: merchants.PaymentEvent{Kind: merchant.KindCaptured, EventID: "41", MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR", OccurredAt: occurred}},
		{name: "refunded", args: outboxwire.EventTypeRefunded, want: merchants.PaymentEvent{Kind: merchant.KindRefunded, EventID: "41", MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR", OccurredAt: occurred}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(outboxwire.Payload{PaymentID: "p-1", MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR", OccurredAt: occurred})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			published, err := publisherWithSchema().message(t.Context(), &outbox.Record{ID: 41, AggregateID: "p-1", EventType: tt.args, Payload: payload})
			if err != nil {
				t.Fatalf("publish: %v", err)
			}
			got, err := decodeEvent(published)
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("movement (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff("p-1", string(published.Key)); diff != "" {
				t.Errorf("every event of a payment is keyed by it (-want +got):\n%s", diff)
			}
		})
	}
}

// A row of a type this version cannot name fails its sweep rather than being skipped: the claim
// takes rows in id order, and skipping one would lose a money event.
func TestARowOfAnUnknownTypeIsNotPublished(t *testing.T) {
	_, err := publisherWithSchema().message(t.Context(), &outbox.Record{ID: 41, AggregateID: "p-1", EventType: "payment.voided", Payload: []byte(`{}`)})
	if !errors.Is(err, ErrEncode) {
		t.Fatalf("err = %v, want %v", err, ErrEncode)
	}
}
