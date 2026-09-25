package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	paymentv1 "github.com/DmytroLysenko1/Kafka-lab/gen/payment/v1"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	schemas "github.com/DmytroLysenko1/Kafka-lab/proto"
)

var (
	ErrRegistry = errors.New("kafka: schema registry")
	ErrBroker   = errors.New("kafka: broker client")
	ErrEncode   = errors.New("kafka: encode outbox record")
	ErrPublish  = errors.New("kafka: publish")
)

// The relay publishes inside the transaction that holds the claimed rows locked, so this
// bounds how long those locks can be held by a broker that has stopped answering.
const deliveryTimeout = 10 * time.Second

type Publisher struct {
	client *kgo.Client
	serde  *sr.Serde
	topic  string
}

// NewPublisher registers the schema the generated code came from and keeps the id the
// registry answered with: every record then carries that id, so a consumer can always find
// the exact schema its bytes were written against.
func NewPublisher(ctx context.Context, brokers []string, topic string, registryURL string) (*Publisher, error) {
	registry, err := sr.NewClient(sr.URLs(registryURL))
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrRegistry, address(registryURL), err)
	}

	subject := topic + "-value"
	registered, err := registry.CreateSchema(ctx, subject, sr.Schema{
		Schema: schemas.PaymentEventsV1,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: register %s: %w", ErrRegistry, subject, err)
	}

	serde := sr.NewSerde()
	serde.Register(registered.ID, &paymentv1.PaymentAuthorized{}, sr.EncodeFn(encodeProto), sr.Index(0))

	// acks=all is what makes a published record survive the loss of the leader that took
	// it; franz-go keeps the producer idempotent unless told otherwise, so the retries it
	// does on the way to that ack cannot duplicate the record in the partition.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBroker, err)
	}

	return &Publisher{
		client: client,
		serde:  serde,
		topic:  topic,
	}, nil
}

func (p *Publisher) Close() {
	p.client.Close()
}

// Publish returns only once every record has been acknowledged, because the caller marks
// the rows published in the transaction it returns to: reporting success before the ack
// would let the relay forget a record the broker never took.
func (p *Publisher) Publish(ctx context.Context, records []outbox.Record) error {
	if len(records) == 0 {
		return nil
	}

	messages := make([]*kgo.Record, 0, len(records))
	for i := range records {
		message, err := p.message(&records[i])
		if err != nil {
			return err
		}
		messages = append(messages, message)
	}

	sending, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()

	if err := p.client.ProduceSync(sending, messages...).FirstErr(); err != nil {
		return fmt.Errorf("%w: %d records to %s: %w", ErrPublish, len(messages), p.topic, err)
	}
	return nil
}

// message keys every record by the aggregate it belongs to, so two events of one payment
// are never handled out of order by landing in different partitions. The outbox row id
// travels as the event id: it survives a republished record, which is what lets a consumer
// recognise the duplicate that at-least-once delivery will eventually hand it.
func (p *Publisher) message(record *outbox.Record) (*kgo.Record, error) {
	if record.EventType != outbox.EventTypeAuthorized {
		return nil, fmt.Errorf("%w %d: unknown event type %q", ErrEncode, record.ID, record.EventType)
	}

	var payload outbox.AuthorizedPayload
	if err := json.Unmarshal(record.Payload, &payload); err != nil {
		return nil, fmt.Errorf("%w %d: %w", ErrEncode, record.ID, err)
	}

	value, err := p.serde.Encode(&paymentv1.PaymentAuthorized{
		PaymentId:   payload.PaymentID,
		MerchantId:  payload.MerchantID,
		AmountMinor: payload.AmountMinor,
		Currency:    payload.Currency,
		OccurredAt:  timestamppb.New(payload.OccurredAt),
	})
	if err != nil {
		return nil, fmt.Errorf("%w %d: %w", ErrEncode, record.ID, err)
	}

	// The record's timestamp is deliberately left for the producer to stamp with the time of
	// publication: it belongs to the log, which uses it for retention and for seeking. The
	// time the payment was authorised is business data and travels inside the payload. Using
	// the event time here costs a whole batch — a broker refuses a timestamp more than
	// log.message.timestamp.after.max.ms (an hour, by default) in the future, so one event
	// dated by a skewed clock would block the relay behind it for good.
	return &kgo.Record{
		Topic: p.topic,
		Key:   []byte(record.AggregateID),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(strconv.FormatInt(record.ID, 10))},
			{Key: "event_type", Value: []byte(record.EventType)},
		},
	}, nil
}

// address names the registry a failure is about without carrying its credentials into a
// log line: a registry url may hold basic-auth, and userinfo and query are where it hides.
func address(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "the configured schema registry"
	}
	return parsed.Scheme + "://" + parsed.Host + parsed.Path
}

func encodeProto(v any) ([]byte, error) {
	message, isProto := v.(proto.Message)
	if !isProto {
		return nil, fmt.Errorf("%w: %T is not a protobuf message", ErrEncode, v)
	}
	return proto.Marshal(message)
}
