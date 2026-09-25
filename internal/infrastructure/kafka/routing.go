package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
)

var (
	ErrDeadLetter = errors.New("kafka: dead letter")
	ErrRetry      = errors.New("kafka: move to a retry tier")
)

const handlingTimeout = 15 * time.Second

type detours interface {
	Retry(ctx context.Context, record *kgo.Record, topic string, attempt int) error
	DeadLetter(ctx context.Context, record *kgo.Record, reason error, class Class) error
}

// handleRecord returns an error only when the record must stay where it is: every other
// outcome — counted, a duplicate, moved to a retry tier, archived — lets the offset move.
func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record) error {
	event, err := decodeEvent(record)
	if err != nil {
		return c.deadLetter(ctx, record, err, ClassUndecodable)
	}

	handling, cancel := context.WithTimeout(ctx, handlingTimeout)
	defer cancel()

	started := time.Now()
	counted, err := c.handle.Execute(handling, event)
	switch {
	case errors.Is(err, merchants.ErrUnprocessable):
		return c.deadLetter(ctx, record, err, ClassRefused)
	case errors.Is(err, merchants.ErrContended):
		return c.moveOn(ctx, record, err)
	case err != nil:
		c.observer.Failed(stageHandle)
		return err
	}
	c.observer.Handled(counted, time.Since(started))

	c.logger.DebugContext(ctx, "record handled",
		"event_id", event.EventID,
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"counted", counted,
	)
	return nil
}

// moveOn takes a contended record off the partition so the payments behind it are not
// held for a lock none of them needs. Past the last tier it has had every chance the chain
// gives, and it is archived for a person to look at.
func (c *Consumer) moveOn(ctx context.Context, record *kgo.Record, reason error) error {
	if c.stage.Next == "" {
		return c.deadLetter(ctx, record, reason, ClassExhausted)
	}
	if err := c.detours.Retry(ctx, record, c.stage.Next, c.stage.Tier+1); err != nil {
		c.observer.Failed(stageRetry)
		return fmt.Errorf("%w: %w", ErrRetry, err)
	}
	c.logger.WarnContext(ctx, "record moved to a retry tier",
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"next", c.stage.Next,
		"reason", reason.Error(),
	)
	return nil
}

// deadLetter is the last way a record leaves the partition unhandled. If the dead letter
// topic itself refuses it, the offset stays put: a record nobody can store is still better
// stuck than silently gone.
func (c *Consumer) deadLetter(ctx context.Context, record *kgo.Record, reason error, class Class) error {
	if err := c.detours.DeadLetter(ctx, record, reason, class); err != nil {
		c.observer.Failed(stageDeadLetter)
		return fmt.Errorf("%w: %w", ErrDeadLetter, err)
	}
	c.logger.ErrorContext(ctx, "record sent to the dead letter topic",
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"class", class,
		"reason", reason.Error(),
	)
	return nil
}
