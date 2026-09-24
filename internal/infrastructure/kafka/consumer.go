package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	paymentv1 "github.com/DmytroLysenko1/Kafka-lab/gen/payment/v1"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
)

var (
	ErrConsume    = errors.New("kafka: consume")
	ErrDecode     = errors.New("kafka: decode record")
	ErrDeadLetter = errors.New("kafka: dead letter")
)

const (
	maxRecordsPerPoll = 100
	handlingTimeout   = 15 * time.Second
	// The commit must survive the signal that stopped the consumer — dropping it would
	// replay work already done — but it must not be able to wait forever on a broker that
	// has stopped answering, or shutdown never finishes and the orchestrator kills it.
	commitTimeout = 5 * time.Second
)

type handler interface {
	Execute(ctx context.Context, event merchants.AuthorizedEvent) (bool, error)
}

type deadLetters interface {
	Send(ctx context.Context, record *kgo.Record, reason string) error
}

type Consumer struct {
	client *kgo.Client
	handle handler
	dead   deadLetters
	logger *slog.Logger
}

// NewConsumer turns autocommit off: the offset must move only after the database has the
// result, and franz-go's autocommit runs on a timer that knows nothing about that.
func NewConsumer(brokers []string, topic, group string, handle handler, dead deadLetters, logger *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// A rebalance in the middle of a polled batch would hand these partitions to another
		// member while this one is still writing their records. Blocking it until the batch
		// is committed is what keeps "handled" and "committed" the same set.
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConsume, err)
	}
	return &Consumer{client: client, handle: handle, dead: dead, logger: logger}, nil
}

func (c *Consumer) Close() { c.client.Close() }

func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := c.poll(ctx); err != nil {
			// A deadline is as much a shutdown as a cancellation: both mean this consumer
			// was told to stop, and neither is a failure to report upwards.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kgo.ErrClientClosed) {
				break
			}
			return err
		}
	}
	return nil
}

// poll handles one batch and commits exactly what it handled. A record it could not handle
// stops the batch with the offset where it was: the next attempt starts there, and nothing
// after it is marked done on the strength of a record that never was.
func (c *Consumer) poll(ctx context.Context) error {
	fetches := c.client.PollRecords(ctx, maxRecordsPerPoll)
	defer c.client.AllowRebalance()

	if err := fetches.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrConsume, err)
	}

	handled := make([]*kgo.Record, 0, maxRecordsPerPoll)
	var failure error

	iterator := fetches.RecordIter()
	for !iterator.Done() {
		record := iterator.Next()
		done, err := c.handleRecord(ctx, record)
		if err != nil {
			failure = err
			break
		}
		if done {
			handled = append(handled, record)
		}
	}

	if len(handled) > 0 {
		committing, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		defer cancel()

		if err := c.client.CommitRecords(committing, handled...); err != nil {
			return fmt.Errorf("%w: commit %d records: %w", ErrConsume, len(handled), err)
		}
	}
	return failure
}

func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record) (bool, error) {
	event, err := decodeEvent(record)
	if err != nil {
		return c.deadLetter(ctx, record, err)
	}

	handling, cancel := context.WithTimeout(ctx, handlingTimeout)
	defer cancel()

	counted, err := c.handle.Execute(handling, event)
	switch {
	case errors.Is(err, merchants.ErrUnprocessable):
		return c.deadLetter(ctx, record, err)
	case err != nil:
		return false, err
	}

	c.logger.DebugContext(ctx, "record handled",
		"event_id", event.EventID,
		"partition", record.Partition,
		"offset", record.Offset,
		"counted", counted,
	)
	return true, nil
}

// deadLetter is the only way a record leaves the partition unhandled. If the dead letter
// topic itself refuses it, the offset stays put: a record nobody can store is still better
// stuck than silently gone.
func (c *Consumer) deadLetter(ctx context.Context, record *kgo.Record, reason error) (bool, error) {
	if err := c.dead.Send(ctx, record, reason.Error()); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDeadLetter, err)
	}
	c.logger.ErrorContext(ctx, "record sent to the dead letter topic",
		"partition", record.Partition,
		"offset", record.Offset,
		"reason", reason.Error(),
	)
	return true, nil
}

// decodeEvent reads the record the way the wire format says to: the schema id and the
// message indexes come off the front, and what remains is the protobuf. The event id comes
// from the header the relay set — the outbox row id, which survives a republished record
// and is therefore what deduplication has to key on.
func decodeEvent(record *kgo.Record) (merchants.AuthorizedEvent, error) {
	header := sr.ConfluentHeader{}

	id, body, err := header.DecodeID(record.Value)
	if err != nil {
		return merchants.AuthorizedEvent{}, fmt.Errorf("%w: schema id: %w", ErrDecode, err)
	}
	_, body, err = header.DecodeIndex(body, 1)
	if err != nil {
		return merchants.AuthorizedEvent{}, fmt.Errorf("%w: schema %d message index: %w", ErrDecode, id, err)
	}

	var authorized paymentv1.PaymentAuthorized
	if err := proto.Unmarshal(body, &authorized); err != nil {
		return merchants.AuthorizedEvent{}, fmt.Errorf("%w: schema %d: %w", ErrDecode, id, err)
	}

	eventID := ""
	for _, header := range record.Headers {
		if header.Key == "event_id" {
			eventID = string(header.Value)
			break
		}
	}

	return merchants.AuthorizedEvent{
		EventID:     eventID,
		MerchantID:  authorized.GetMerchantId(),
		AmountMinor: authorized.GetAmountMinor(),
	}, nil
}
