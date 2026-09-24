package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrDeadLetterTopic = errors.New("kafka: dead letter topic")

// DeadLetters archives a record nobody can process, bytes unchanged. Nothing here decodes
// it: a record reaches this topic precisely because decoding it failed, and the person who
// comes to look at it needs what actually arrived, not this service's idea of it.
type DeadLetters struct {
	client *kgo.Client
	topic  string
}

func NewDeadLetters(brokers []string, topic string) (*DeadLetters, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDeadLetterTopic, err)
	}
	return &DeadLetters{client: client, topic: topic}, nil
}

func (d *DeadLetters) Close() { d.client.Close() }

func (d *DeadLetters) Send(ctx context.Context, record *kgo.Record, reason string) error {
	archived := &kgo.Record{
		Topic: d.topic,
		Key:   record.Key,
		Value: record.Value,
		Headers: append(record.Headers,
			kgo.RecordHeader{Key: "dlq_reason", Value: []byte(reason)},
			kgo.RecordHeader{Key: "dlq_origin_topic", Value: []byte(record.Topic)},
			kgo.RecordHeader{Key: "dlq_origin_partition", Value: []byte(fmt.Sprint(record.Partition))},
			kgo.RecordHeader{Key: "dlq_origin_offset", Value: []byte(fmt.Sprint(record.Offset))},
			kgo.RecordHeader{Key: "dlq_archived_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		),
	}

	sending, cancel := context.WithTimeout(context.WithoutCancel(ctx), deliveryTimeout)
	defer cancel()

	if err := d.client.ProduceSync(sending, archived).FirstErr(); err != nil {
		return fmt.Errorf("%w: %w", ErrDeadLetterTopic, err)
	}
	return nil
}
