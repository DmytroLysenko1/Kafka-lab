package merchants

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
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
	Add(ctx context.Context, authorization merchant.Authorization, at time.Time) error
}

type txManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type Clock func() time.Time

// AuthorizedEvent is the event as it came off the topic, not yet trusted: turning it into
// a merchant.Authorization is what decides whether it can be counted at all.
type AuthorizedEvent struct {
	EventID     string
	MerchantID  string
	AmountMinor int64
}

type RecordAuthorized struct {
	inbox  inboxStore
	totals totalsStore
	tx     txManager
	now    Clock
}

func NewRecordAuthorized(inbox inboxStore, totals totalsStore, tx txManager, now Clock) *RecordAuthorized {
	return &RecordAuthorized{inbox: inbox, totals: totals, tx: tx, now: now}
}

// Execute reports whether the event was counted; a redelivery is not an error, it is the
// normal cost of at-least-once delivery. An event the domain refuses is unprocessable: no
// redelivery will make it countable.
func (uc *RecordAuthorized) Execute(ctx context.Context, event AuthorizedEvent) (bool, error) {
	authorization, err := merchant.NewAuthorization(event.EventID, event.MerchantID, event.AmountMinor)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrUnprocessable, err)
	}

	counted, err := uc.countOnce(ctx, authorization)
	if err != nil {
		return false, fmt.Errorf("merchants: record authorised event %s: %w", authorization.EventID(), err)
	}
	return counted, nil
}

// countOnce writes the claim and the total together, so a failure after the claim cannot
// leave an event marked handled that nobody ever counted.
func (uc *RecordAuthorized) countOnce(ctx context.Context, authorization merchant.Authorization) (bool, error) {
	counted := false
	at := uc.now()

	err := uc.tx.WithinTx(ctx, func(ctx context.Context) error {
		first, err := uc.inbox.Claim(ctx, authorization.EventID(), at)
		if err != nil || !first {
			return err
		}
		if err := uc.totals.Add(ctx, authorization, at); err != nil {
			return err
		}
		counted = true
		return nil
	})
	return counted, err
}
