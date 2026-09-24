package merchants

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrUnprocessable marks an event that will never succeed, however many times it is
// delivered. A consumer needs that distinction: a database that is down deserves the offset
// held and another attempt, while a record that cannot be understood must leave the
// partition for the dead letter topic instead of blocking everything behind it.
var ErrUnprocessable = errors.New("merchants: the event cannot be processed")

var (
	ErrEventIDRequired    = errors.New("merchants: the event carries no id to deduplicate it by")
	ErrMerchantRequired   = errors.New("merchants: the event names no merchant")
	ErrAmountNotPositive  = errors.New("merchants: an authorised amount is positive")
	ErrAmountOutOfBalance = errors.New("merchants: the amount does not fit the running total")
	ErrIdentifierTooLong  = errors.New("merchants: the event carries an identifier longer than any real one")
)

// maxAuthorizedMinor is a sanity bound, not a business rule: it leaves room for a total to
// be added to without ever reaching the point where int64 wraps and a merchant's balance
// turns negative on the way past the largest amount Postgres can hold.
const maxAuthorizedMinor = 1 << 50

// maxIdentifierLength matches what the producing domain allows a merchant id to be. Both
// identifiers become primary keys here, and both arrive over a topic: a producer at any
// version — or one that is not ours at all — can put a megabyte in a header, and a
// megabyte-wide key would be stored, indexed and kept forever.
const maxIdentifierLength = 64

type inboxStore interface {
	Claim(ctx context.Context, eventID string, at time.Time) (bool, error)
}

type totalsStore interface {
	Add(ctx context.Context, merchantID string, minor int64, at time.Time) error
}

type txManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type Clock func() time.Time

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
// normal cost of at-least-once delivery. The claim and the total are written together, so a
// failure after the claim cannot leave an event marked handled that nobody ever counted.
func (uc *RecordAuthorized) Execute(ctx context.Context, event AuthorizedEvent) (bool, error) {
	if err := validate(event); err != nil {
		return false, err
	}

	counted := false
	at := uc.now()

	err := uc.tx.WithinTx(ctx, func(ctx context.Context) error {
		first, err := uc.inbox.Claim(ctx, event.EventID, at)
		if err != nil {
			return err
		}
		if !first {
			return nil
		}
		if err := uc.totals.Add(ctx, event.MerchantID, event.AmountMinor, at); err != nil {
			return err
		}
		counted = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("merchants: record authorised event %s: %w", event.EventID, err)
	}
	return counted, nil
}

// The producer's domain guarantees a positive amount, but the consumer reads bytes off a
// topic: anything that ever published to it, at any version, is what actually arrives.
func validate(event AuthorizedEvent) error {
	var refusal error
	switch {
	case event.EventID == "":
		refusal = ErrEventIDRequired
	case event.MerchantID == "":
		refusal = ErrMerchantRequired
	case len(event.EventID) > maxIdentifierLength, len(event.MerchantID) > maxIdentifierLength:
		refusal = ErrIdentifierTooLong
	case event.AmountMinor <= 0:
		refusal = ErrAmountNotPositive
	case event.AmountMinor > maxAuthorizedMinor:
		refusal = ErrAmountOutOfBalance
	default:
		return nil
	}
	return fmt.Errorf("%w: %w", ErrUnprocessable, refusal)
}
