package kafka

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	paymentv1 "github.com/DmytroLysenko1/Kafka-lab/gen/payment/v1"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/outboxwire"
)

var ErrDecode = errors.New("kafka: decode record")

// movementMessage is what the three payment messages have in common: they differ in what
// happened, not in what they carry.
type movementMessage interface {
	proto.Message
	GetMerchantId() string
	GetAmountMinor() int64
	GetCurrency() string
	GetOccurredAt() *timestamppb.Timestamp
}

// wireEvent is one message of the schema, both ways: the message type at its index, the kind
// of movement it is read as, the event_type the relay stamps on it, and how the relay builds it
// from an outbox row. One table, so an event type is either written and read or neither.
type wireEvent struct {
	kind      merchant.Kind
	eventType string
	message   func() movementMessage
	build     func(*outboxwire.Payload) proto.Message
}

// wireEvents is indexed by the message's position in proto/payment/v1/events.proto, which is
// its index on the wire (the Confluent framing): the one place a record's bytes are given a
// meaning.
func wireEvents() []wireEvent {
	return []wireEvent{
		{
			kind:      merchant.KindAuthorized,
			eventType: outboxwire.EventTypeAuthorized,
			message:   func() movementMessage { return &paymentv1.PaymentAuthorized{} },
			build: func(p *outboxwire.Payload) proto.Message {
				return &paymentv1.PaymentAuthorized{PaymentId: p.PaymentID, MerchantId: p.MerchantID, AmountMinor: p.AmountMinor, Currency: p.Currency, OccurredAt: timestamppb.New(p.OccurredAt)}
			},
		},
		{
			kind:      merchant.KindCaptured,
			eventType: outboxwire.EventTypeCaptured,
			message:   func() movementMessage { return &paymentv1.PaymentCaptured{} },
			build: func(p *outboxwire.Payload) proto.Message {
				return &paymentv1.PaymentCaptured{PaymentId: p.PaymentID, MerchantId: p.MerchantID, AmountMinor: p.AmountMinor, Currency: p.Currency, OccurredAt: timestamppb.New(p.OccurredAt)}
			},
		},
		{
			kind:      merchant.KindRefunded,
			eventType: outboxwire.EventTypeRefunded,
			message:   func() movementMessage { return &paymentv1.PaymentRefunded{} },
			build: func(p *outboxwire.Payload) proto.Message {
				return &paymentv1.PaymentRefunded{PaymentId: p.PaymentID, MerchantId: p.MerchantID, AmountMinor: p.AmountMinor, Currency: p.Currency, OccurredAt: timestamppb.New(p.OccurredAt)}
			},
		},
	}
}

// wireEventNamed is the message the relay publishes a row of eventType as.
func wireEventNamed(eventType string) (wireEvent, bool) {
	events := wireEvents()
	index := slices.IndexFunc(events, func(event wireEvent) bool {
		return event.eventType == eventType
	})
	if index < 0 {
		return wireEvent{}, false
	}
	return events[index], true
}

// decodeEvent reads the record the way the wire format says to: the schema id and the
// message index come off the front, and what remains is the protobuf. The event id comes
// from the header the relay set — the outbox row id, which survives a republished record
// and is therefore what deduplication has to key on.
func decodeEvent(record *kgo.Record) (merchants.PaymentEvent, error) {
	id, index, body, err := unframe(record.Value)
	if err != nil {
		return merchants.PaymentEvent{}, err
	}
	event, err := eventAt(index, headerValue(record.Headers, headerEventType))
	if err != nil {
		return merchants.PaymentEvent{}, fmt.Errorf("%w: schema %d: %w", ErrDecode, id, err)
	}

	message := event.message()
	if err := proto.Unmarshal(body, message); err != nil {
		return merchants.PaymentEvent{}, fmt.Errorf("%w: schema %d: %w", ErrDecode, id, err)
	}
	return merchants.PaymentEvent{
		Kind:        event.kind,
		EventID:     headerValue(record.Headers, headerEventID),
		MerchantID:  message.GetMerchantId(),
		AmountMinor: message.GetAmountMinor(),
		Currency:    message.GetCurrency(),
		// An event without the field reads as the zero time, which is too old to count:
		// refused, not guessed at.
		OccurredAt: occurredAt(message.GetOccurredAt()),
	}, nil
}

// unframe takes the schema id and the message index off the front of a value, leaving the
// protobuf.
func unframe(value []byte) (int, []int, []byte, error) {
	header := sr.ConfluentHeader{}
	id, body, err := header.DecodeID(value)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("%w: schema id: %w", ErrDecode, err)
	}
	index, body, err := header.DecodeIndex(body, 1)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("%w: schema %d message index: %w", ErrDecode, id, err)
	}
	return id, index, body, nil
}

var errNotAPaymentEvent = errors.New("not a payment event")

// eventAt is the message at index, if the record agrees about what it is. A record that says
// it is something else is not counted as that message merely because its bytes happen to
// parse as one — the three messages share their field numbers, so any of them parses as any
// other. A record without the event_type header is read by its index alone, so records
// published before the relay stamped the header stay readable.
func eventAt(index []int, eventType string) (wireEvent, error) {
	events := wireEvents()
	if len(index) != 1 || index[0] < 0 || index[0] >= len(events) {
		return wireEvent{}, fmt.Errorf("message index %v: %w", index, errNotAPaymentEvent)
	}
	event := events[index[0]]
	if eventType != "" && eventType != event.eventType {
		return wireEvent{}, fmt.Errorf("event type %q on message %v, which is %s: %w", eventType, index, event.eventType, errNotAPaymentEvent)
	}
	return event, nil
}

func occurredAt(at *timestamppb.Timestamp) time.Time {
	if at == nil {
		return time.Time{}
	}
	return at.AsTime()
}
