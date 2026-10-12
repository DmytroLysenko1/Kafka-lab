package merchants

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/retention"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

// ErrUnprocessable marks an event that will never succeed, however many times it is
// delivered. A consumer needs that distinction: a database that is down deserves the offset
// held and another attempt, while a record that cannot be understood must leave the
// partition for the dead letter topic instead of blocking everything behind it.
var ErrUnprocessable = errors.New("merchants: the event cannot be processed")

// ErrContended marks an event that could not be counted now because another transaction
// holds the row it has to write, and that is expected to succeed later. It is the one
// failure that belongs to this record alone: stopping the partition for it would make
// every payment behind it wait for a lock none of them needs.
var ErrContended = errors.New("merchants: the merchant's total is held by another transaction")

type inboxStore interface {
	Claim(ctx context.Context, eventID string, at time.Time) (bool, error)
}

type totalsStore interface {
	Add(ctx context.Context, movement merchant.Movement, at time.Time) error
}

type txManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type Clock func() time.Time

// PaymentEvent is an event as it came off the topic, not yet trusted: turning it into a
// merchant.Movement is what decides whether it can be counted at all. Kind is what the
// decoder read the event as — an authorisation, a capture or a refund.
type PaymentEvent struct {
	Kind        merchant.Kind
	EventID     string
	MerchantID  string
	AmountMinor int64
	Currency    string
	OccurredAt  time.Time
}

// RecordMovement counts one payment event into its merchant's totals, once.
type RecordMovement struct {
	inbox  inboxStore
	totals totalsStore
	tx     txManager
	now    Clock
}

func NewRecordMovement(inbox inboxStore, totals totalsStore, tx txManager, now Clock) *RecordMovement {
	return &RecordMovement{
		inbox:  inbox,
		totals: totals,
		tx:     tx,
		now:    now,
	}
}

// Execute reports whether the event was counted; a redelivery is not an error, it is the
// normal cost of at-least-once delivery. An event the domain refuses is unprocessable: no
// redelivery will make it countable.
func (uc *RecordMovement) Execute(ctx context.Context, event *PaymentEvent) (bool, error) {
	ctx, end := telemetry.Start(ctx, "merchants.RecordMovement")
	counted, err := uc.record(ctx, event)
	end(err)
	return counted, err
}

func (uc *RecordMovement) record(ctx context.Context, event *PaymentEvent) (bool, error) {
	movement, err := movementOf(event)
	if err != nil {
		return false, fmt.Errorf("merchants: record %s event: %w", event.Kind, errors.Join(ErrUnprocessable, err))
	}
	at := uc.now()
	if !retention.CanBeDeduplicated(event.OccurredAt, at) {
		return false, fmt.Errorf("merchants: record %s event %s from %s: %w",
			movement.Kind(), movement.EventID(), event.OccurredAt, errors.Join(ErrUnprocessable, retention.ErrTooOldToDeduplicate))
	}

	counted, err := uc.countOnce(ctx, movement, at)
	if err != nil {
		return false, fmt.Errorf("merchants: record %s event %s: %w", movement.Kind(), movement.EventID(), err)
	}
	return counted, nil
}

// movementOf trusts nothing about the event: an amount without a currency it can be kept in
// is as uncountable as one without a merchant, or one of no kind a total counts.
func movementOf(event *PaymentEvent) (merchant.Movement, error) {
	amount, err := merchant.NewAmount(event.AmountMinor, event.Currency)
	if err != nil {
		return merchant.Movement{}, err
	}
	return merchant.NewMovement(event.Kind, event.EventID, event.MerchantID, amount)
}

// countOnce writes the claim and the total together, so a failure after the claim cannot
// leave an event marked handled that nobody ever counted.
func (uc *RecordMovement) countOnce(ctx context.Context, movement merchant.Movement, at time.Time) (bool, error) {
	counted := false

	err := uc.tx.WithinTx(ctx, func(ctx context.Context) error {
		first, err := uc.inbox.Claim(ctx, movement.EventID(), at)
		if err != nil || !first {
			return err
		}
		if err := uc.totals.Add(ctx, movement, at); err != nil {
			return err
		}
		counted = true
		return nil
	})
	return counted, err
}
