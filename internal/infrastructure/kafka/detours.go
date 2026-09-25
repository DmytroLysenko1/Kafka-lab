package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrDetours         = errors.New("kafka: detours producer")
	ErrDeadLetterTopic = errors.New("kafka: dead letter topic")
	ErrRetryTopic      = errors.New("kafka: retry topic")
)

// Detours is every way a record leaves its stage without being handled there: moved on to
// the next retry tier, or archived in the dead letter topic. Either way the bytes go
// unchanged. Nothing here decodes a record: a record often takes a detour precisely
// because decoding it failed, and whoever comes to look at it needs what actually arrived,
// not this service's idea of it.
type Detours struct {
	client   *kgo.Client
	dlqTopic string
	observer detoursObserver
}

// detoursObserver hears where records were sent, from the one place that knows it sent them.
type detoursObserver interface {
	Retried(tier string)
	DeadLettered(class string)
}

func NewDetours(brokers []string, dlqTopic string, watch detoursObserver) (*Detours, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDetours, err)
	}
	return &Detours{
		client:   client,
		dlqTopic: dlqTopic,
		observer: watch,
	}, nil
}

func (d *Detours) Close() {
	d.client.Close()
}

// Retry moves a record into a retry tier as its given attempt. The key goes with it, so a
// tier keeps the same payment on the same partition number it had before.
func (d *Detours) Retry(ctx context.Context, record *kgo.Record, topic string, attempt int) error {
	moved := &kgo.Record{
		Topic:   topic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: retryHeaders(record, attempt, time.Now()),
	}
	if err := d.produce(ctx, moved); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrRetryTopic, topic, err)
	}
	d.observer.Retried(topic)
	return nil
}

// Replay puts a dead letter back at the start of a chain, as a first attempt.
func (d *Detours) Replay(ctx context.Context, record *kgo.Record, topic string) error {
	replayed := &kgo.Record{
		Topic:   topic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: replayHeaders(record),
	}
	if err := d.produce(ctx, replayed); err != nil {
		return fmt.Errorf("%w: replay into %s: %w", ErrRetryTopic, topic, err)
	}
	return nil
}

// DeadLetter archives a record with the reason and a class a replay can be filtered by.
func (d *Detours) DeadLetter(ctx context.Context, record *kgo.Record, reason error, class Class) error {
	archived := &kgo.Record{
		Topic:   d.dlqTopic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: deadLetterHeaders(record, reason, class, time.Now()),
	}
	if err := d.produce(ctx, archived); err != nil {
		return fmt.Errorf("%w: %w", ErrDeadLetterTopic, err)
	}
	d.observer.DeadLettered(string(class))
	return nil
}

// deadLetterHeaders replaces every dlq_ header rather than appending it: a producer that
// put its own dlq_reason on a record would otherwise have it read first, ahead of ours.
func deadLetterHeaders(record *kgo.Record, reason error, class Class, archivedAt time.Time) []kgo.RecordHeader {
	headers := withHeader(record.Headers, headerDeadLetterClass, string(class))
	headers = withHeader(headers, headerDeadLetterReason, boundedReason(reason))
	headers = withHeader(headers, headerDeadLetterOriginTopic, record.Topic)
	headers = withHeader(headers, headerDeadLetterOriginPartition, strconv.Itoa(int(record.Partition)))
	headers = withHeader(headers, headerDeadLetterOriginOffset, strconv.FormatInt(record.Offset, 10))
	return withHeader(headers, headerDeadLetterArchivedAt, archivedAt.UTC().Format(time.RFC3339))
}

// produce outlives the caller's cancellation on purpose: the offset behind this record is
// committed only once the copy is stored, and a copy abandoned halfway by a shutdown would
// be sent again on restart anyway. It is still bounded, so a broker that stopped answering
// cannot hold the shutdown forever.
func (d *Detours) produce(ctx context.Context, record *kgo.Record) error {
	sending, cancel := context.WithTimeout(context.WithoutCancel(ctx), deliveryTimeout)
	defer cancel()

	return d.client.ProduceSync(sending, record).FirstErr()
}
