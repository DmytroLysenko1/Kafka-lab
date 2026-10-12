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

// Handling a record is one short transaction whose lock wait is itself capped at 2 s
// (lock_timeout in postgres/totals.go); 15 s is the ceiling for a database that is slow rather
// than locked, and the largest share that still fits the consumer's 58 s (consumer.go).
const handlingTimeout = 15 * time.Second

type detours interface {
	Retry(ctx context.Context, record *kgo.Record, topic string, attempt int) error
	DeadLetter(ctx context.Context, record *kgo.Record, reason error, class Class) error
}

// handleRecord returns an error only when the record must stay where it is: every other
// outcome — counted, a duplicate, moved to a retry tier, archived — lets the offset move.
func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record) error {
	ctx, span := processing(ctx, c.tracer, record)
	defer span.End()

	event, err := decodeEvent(record)
	if err != nil {
		return c.deadLetter(ctx, record, err, ClassUndecodable)
	}

	counted, err := c.count(ctx, &event)
	switch {
	case stopping(err):
		// The record stays where it is and is fetched again after the restart; a rollout
		// would otherwise show up on the failure panel every time.
		return err
	case errors.Is(err, merchants.ErrUnprocessable):
		return c.deadLetter(ctx, record, err, ClassRefused)
	case errors.Is(err, merchants.ErrContended):
		return c.moveOn(ctx, record, err)
	case err != nil:
		c.countFailure(stageHandle, err)
		return err
	}
	c.observer.Handled(handlingOutcome(counted, nil))
	c.logHandled(ctx, record, &event, counted)
	return nil
}

// count runs the use case under handlingTimeout and times the attempt, whatever it came to.
func (c *Consumer) count(ctx context.Context, event *merchants.PaymentEvent) (bool, error) {
	handling, cancel := context.WithTimeout(ctx, handlingTimeout)
	defer cancel()

	started := time.Now()
	counted, err := c.handle.Execute(handling, event)
	if !stopping(err) {
		c.observer.HandlingTook(handlingOutcome(counted, err), time.Since(started))
	}
	return counted, err
}

func (c *Consumer) logHandled(ctx context.Context, record *kgo.Record, event *merchants.PaymentEvent, counted bool) {
	c.logger.DebugContext(ctx, "record handled",
		"event_id", event.EventID,
		"kind", event.Kind.String(),
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"counted", counted,
	)
}

// handlingOutcome names what one attempt at a record came to, for the handling-time
// histogram: a contended record waited out the lock timeout and a failed one may have waited
// out handlingTimeout, and a histogram that saw only the successes would never show either.
func handlingOutcome(counted bool, err error) string {
	switch {
	case errors.Is(err, merchants.ErrUnprocessable):
		return outcomeRefused
	case errors.Is(err, merchants.ErrContended):
		return outcomeContended
	case err != nil:
		return outcomeFailed
	case counted:
		return outcomeCounted
	}
	return outcomeDuplicate
}

// moveOn takes a contended record off the partition so the payments behind it are not
// held for a lock none of them needs. Past the last tier it has had every chance the chain
// gives, and it is archived for a person to look at.
func (c *Consumer) moveOn(ctx context.Context, record *kgo.Record, reason error) error {
	if c.stage.Next == "" {
		return c.deadLetter(ctx, record, reason, ClassExhausted)
	}
	if err := c.detours.Retry(ctx, record, c.stage.Next, c.stage.Tier+1); err != nil {
		c.countFailure(stageRetry, err)
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
		c.countFailure(stageDeadLetter, err)
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

// countFailure reports a failed stage unless the error is the consumer stopping: a detour
// refused because the process is shutting down is not a detour that failed.
func (c *Consumer) countFailure(stage string, err error) {
	if !stopping(err) {
		c.observer.Failed(stage)
	}
}
