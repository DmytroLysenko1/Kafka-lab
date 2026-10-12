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

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/outboxwire"
	schemas "github.com/DmytroLysenko1/Kafka-lab/proto"
)

var (
	ErrRegistry = errors.New("kafka: schema registry")
	ErrBroker   = errors.New("kafka: broker client")
	ErrEncode   = errors.New("kafka: encode outbox record")
	ErrPublish  = errors.New("kafka: publish")
)

// The relay publishes inside the transaction that holds the claimed rows locked, so this
// bounds how long a broker that is gone can keep those locks held. A broker that hangs with a
// request already sent can hold it to the request's own deadline, about 20 s by franz-go's
// defaults, since an idempotent record in flight cannot be failed early; the sweep's 30 s
// sweepTimeout (cmd/outbox-relay) is the ceiling then.
const deliveryTimeout = 10 * time.Second

// The service that owns the schema declares how it may evolve, before it registers a
// version: Apicurio starts a new subject at NONE (exp-12), which accepts any change, and
// the check the architecture relies on would not exist. BACKWARD — a new schema must read
// what the previous one wrote — is the level that matches deploying consumers first (09).
const schemaCompatibility = sr.CompatBackward

type Publisher struct {
	client *kgo.Client
	serde  *sr.Serde
	topic  string
}

// NewPublisher registers the schema the generated code came from and keeps the id the
// registry answered with: every record then carries that id, so a consumer can always find
// the exact schema its bytes were written against.
func NewPublisher(ctx context.Context, brokers []string, topic string, registryURL string) (*Publisher, error) {
	serde, err := registerSchema(ctx, registryURL, topic+"-value")
	if err != nil {
		return nil, err
	}
	client, err := newProducer(brokers, topic)
	if err != nil {
		return nil, err
	}
	return &Publisher{
		client: client,
		serde:  serde,
		topic:  topic,
	}, nil
}

func registerSchema(ctx context.Context, registryURL, subject string) (*sr.Serde, error) {
	registry, err := sr.NewClient(sr.URLs(registryURL))
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrRegistry, address(registryURL), err)
	}
	if err := requireCompatibility(ctx, registry, subject); err != nil {
		return nil, err
	}
	registered, err := registry.CreateSchema(ctx, subject, sr.Schema{
		Schema: schemas.PaymentEventsV1,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: register %s: %w", ErrRegistry, subject, err)
	}

	serde := sr.NewSerde()
	for index, event := range wireEvents() {
		serde.Register(registered.ID, event.message(), sr.EncodeFn(encodeProto), sr.Index(index))
	}
	return serde, nil
}

// newProducer asks for acks=all, which is what makes a published record survive the loss of
// the leader that took it; franz-go keeps the producer idempotent unless told otherwise, so the
// retries it does on the way to that ack cannot duplicate the record in the partition.
func newProducer(brokers []string, topic string) (*kgo.Client, error) {
	_, traced := tracing()
	client, err := kgo.NewClient(
		traced,
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBroker, err)
	}
	return client, nil
}

func requireCompatibility(ctx context.Context, registry *sr.Client, subject string) error {
	result := registry.SetCompatibility(ctx, sr.SetCompatibility{
		Level: schemaCompatibility,
	}, subject)
	if err := result[0].Err; err != nil {
		return fmt.Errorf("%w: set %s compatibility: %w", ErrRegistry, subject, err)
	}
	return nil
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

	sending, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()

	messages := make([]*kgo.Record, 0, len(records))
	for i := range records {
		message, err := p.message(sending, &records[i])
		if err != nil {
			return err
		}
		messages = append(messages, message)
	}

	if err := p.client.ProduceSync(sending, messages...).FirstErr(); err != nil {
		return fmt.Errorf("%w: %d records to %s: %w", ErrPublish, len(messages), p.topic, err)
	}
	return nil
}

// message keys every record by the aggregate it belongs to, so two events of one payment
// are never handled out of order by landing in different partitions. The outbox row id
// travels as the event id: it survives a republished record, which is what lets a consumer
// recognise the duplicate that at-least-once delivery will eventually hand it.
//
// A row this version cannot encode fails the whole sweep, and every sweep after it: the claim
// takes rows in id order, and skipping one would lose a money event. So a new event type ships
// in the relay before any API writes it (docs/k8s/decisions.md, "Rollout order"); a backlog
// that grows while the broker is healthy and sweeps fail is this, and a relay release is the fix.
func (p *Publisher) message(ctx context.Context, record *outbox.Record) (*kgo.Record, error) {
	value, err := p.encode(record)
	if err != nil {
		return nil, err
	}

	// The record's timestamp is deliberately left for the producer to stamp with the time of
	// publication: it belongs to the log, which uses it for retention and for seeking. The
	// time the payment was authorised is business data and travels inside the payload. Using
	// the event time here costs a whole batch — a broker refuses a timestamp more than
	// log.message.timestamp.after.max.ms (an hour, by default) in the future, so one event
	// dated by a skewed clock would block the relay behind it for good.
	return &kgo.Record{
		Topic:   p.topic,
		Key:     []byte(record.AggregateID),
		Value:   value,
		Context: raisedIn(ctx, record.TraceContext),
		Headers: []kgo.RecordHeader{
			{Key: headerEventID, Value: []byte(strconv.FormatInt(record.ID, 10))},
			{Key: headerEventType, Value: []byte(record.EventType)},
		},
	}, nil
}

// encode turns the outbox's own format into the message its event type names, from the table
// the consumer reads with (decode.go).
func (p *Publisher) encode(record *outbox.Record) ([]byte, error) {
	event, known := wireEventNamed(record.EventType)
	if !known {
		return nil, fmt.Errorf("%w %d: unknown event type %q", ErrEncode, record.ID, record.EventType)
	}
	var payload outboxwire.Payload
	if err := json.Unmarshal(record.Payload, &payload); err != nil {
		return nil, fmt.Errorf("%w %d: %w", ErrEncode, record.ID, err)
	}
	value, err := p.serde.Encode(event.build(&payload))
	if err != nil {
		return nil, fmt.Errorf("%w %d: %w", ErrEncode, record.ID, err)
	}
	return value, nil
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
