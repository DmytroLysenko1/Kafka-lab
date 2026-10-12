package payment

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

var (
	ErrMerchantRequired  = errors.New("payment: merchant id is required")
	ErrMerchantTooLong   = errors.New("payment: merchant id is longer than allowed")
	ErrIDFormat          = errors.New("payment: id is not a uuid")
	ErrAmountNotPositive = errors.New("payment: an authorised payment is for a positive amount")
	ErrAmountTooLarge    = errors.New("payment: the amount is larger than one authorisation may be")
	ErrMerchantInvalid   = errors.New("payment: merchant id is not valid UTF-8 text")
	ErrStorage           = errors.New("payment: storage failure")
	ErrOutcomeUnknown    = errors.New("payment: the storage did not say whether the payment was kept")
	ErrCorrupt           = errors.New("payment: a stored payment or its event cannot be read or written as one")
	ErrNotTheSamePayment = errors.New("payment: a replay asks for a different payment than the one it repeats")
	ErrNotFound          = errors.New("payment: no such payment for this merchant")
	ErrInvalidTransition = errors.New("payment: the payment is not in a state that allows this")
	// ErrConcurrentModification is a payment whose status changed between being read and being
	// written, which the row lock taken on read is there to make impossible: seeing it means
	// something wrote the row without taking that lock.
	ErrConcurrentModification = errors.New("payment: the payment changed while it was being changed")
)

const merchantIDMaxLength = 64

type ID struct {
	value uuid.UUID
}

func NewID() ID {
	return ID{value: uuid.New()}
}

func ParseID(value string) (ID, error) {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return ID{}, ErrIDFormat
	}
	return ID{
		value: parsed,
	}, nil
}

func (id ID) String() string {
	return id.value.String()
}

type MerchantID struct {
	value string
}

func ParseMerchantID(value string) (MerchantID, error) {
	switch {
	case value == "":
		return MerchantID{}, ErrMerchantRequired
	case len(value) > merchantIDMaxLength:
		return MerchantID{}, ErrMerchantTooLong
	case !isStorableText(value):
		return MerchantID{}, ErrMerchantInvalid
	}
	return MerchantID{
		value: value,
	}, nil
}

func (m MerchantID) String() string {
	return m.value
}

// isStorableText refuses what Postgres refuses to store in a text column — a NUL byte and
// bytes that are not UTF-8. Refused here it is a 400 the caller can act on; refused by the
// database it would be a storage failure, and the same payment would fail on every retry.
func isStorableText(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

type Status uint8

const (
	StatusUnknown Status = iota
	StatusAuthorized
	StatusCaptured
	StatusRefunded
)

// String is the status as the domain names it — in logs, traces and answers. How a status is
// spelled in a table is the store's business (postgres/statuses.go).
func (s Status) String() string {
	switch s {
	case StatusAuthorized:
		return "authorized"
	case StatusCaptured:
		return "captured"
	case StatusRefunded:
		return "refunded"
	}
	return "unknown"
}

// History is when a payment reached each status it has been in. A status not reached yet is
// the zero time. It is a value: changing it makes another one.
type History struct {
	authorizedAt time.Time
	capturedAt   time.Time
	refundedAt   time.Time
}

// NewHistory holds every time in UTC, and a status not reached as the zero time.
func NewHistory(authorizedAt, capturedAt, refundedAt time.Time) History {
	return History{
		authorizedAt: utcOrZero(authorizedAt),
		capturedAt:   utcOrZero(capturedAt),
		refundedAt:   utcOrZero(refundedAt),
	}
}

func (h History) AuthorizedAt() time.Time {
	return h.authorizedAt
}

func (h History) CapturedAt() time.Time {
	return h.capturedAt
}

func (h History) RefundedAt() time.Time {
	return h.refundedAt
}

type Payment struct {
	id       ID
	merchant MerchantID
	amount   Money
	status   Status
	history  History
	events   []Event
}

// Authorize is the only way a Payment comes into existence, so an amount of zero cannot be
// authorised even though Money itself allows it: a zero-amount authorisation reserves
// nothing and would still emit an event downstream consumers would act on.
func Authorize(id ID, merchant MerchantID, amount Money, at time.Time) (*Payment, error) {
	if !amount.IsPositive() {
		return nil, ErrAmountNotPositive
	}
	if !amount.IsWithinAuthorizationLimit() {
		return nil, ErrAmountTooLarge
	}

	authorized := &Payment{
		id:       id,
		merchant: merchant,
		amount:   amount,
		status:   StatusAuthorized,
		history:  NewHistory(at, time.Time{}, time.Time{}),
	}
	authorized.events = append(authorized.events, &Authorized{
		movement: authorized.movement(authorized.history.AuthorizedAt()),
	})
	return authorized, nil
}

// Reconstitute rebuilds a payment that was authorised earlier and stored. It emits nothing:
// the payment's events were published when it came into existence, and rebuilding it to
// compare a replay against must not announce it a second time.
func Reconstitute(id ID, merchant MerchantID, amount Money, status Status, history History) *Payment {
	return &Payment{
		id:       id,
		merchant: merchant,
		amount:   amount,
		status:   status,
		history:  history,
	}
}

func utcOrZero(at time.Time) time.Time {
	if at.IsZero() {
		return time.Time{}
	}
	return at.UTC()
}

func (p *Payment) ID() ID {
	return p.id
}

func (p *Payment) Merchant() MerchantID {
	return p.merchant
}

func (p *Payment) Amount() Money {
	return p.amount
}

func (p *Payment) Status() Status {
	return p.status
}

func (p *Payment) History() History {
	return p.history
}

// Capture takes the whole authorised amount. Capturing a captured payment again changes
// nothing and announces nothing — the caller repeating a capture whose answer it lost gets the
// same payment back, not a second capture. A refunded payment cannot be captured again.
func (p *Payment) Capture(at time.Time) error {
	switch p.status {
	case StatusCaptured:
		return nil
	case StatusAuthorized:
	default:
		return ErrInvalidTransition
	}
	p.status = StatusCaptured
	p.history.capturedAt = at.UTC()
	p.events = append(p.events, &Captured{
		movement: p.movement(p.history.capturedAt),
	})
	return nil
}

// Refund returns the whole captured amount. Only a captured payment can be refunded: an
// authorisation that was never captured took no money to give back. Refunding twice changes
// nothing, for the same reason capturing twice does not.
func (p *Payment) Refund(at time.Time) error {
	switch p.status {
	case StatusRefunded:
		return nil
	case StatusCaptured:
	default:
		return ErrInvalidTransition
	}
	p.status = StatusRefunded
	p.history.refundedAt = at.UTC()
	p.events = append(p.events, &Refunded{
		movement: p.movement(p.history.refundedAt),
	})
	return nil
}

// Replays says whether this authorisation may stand as a replay of an earlier one, and
// refuses with ErrNotTheSamePayment when it asks for something else. What "the same
// payment" means is the payment's own business — the merchant and the amount, currency
// included — and a caller comparing fields from outside would have to be told again on
// the day an authorisation grows one.
func (p *Payment) Replays(earlier *Payment) error {
	if p.merchant != earlier.merchant || p.amount != earlier.amount {
		return ErrNotTheSamePayment
	}
	return nil
}

// PullEvents hands the pending events over and forgets them, so the repository that stores
// the aggregate is the only place that can publish them and can only do it once.
func (p *Payment) PullEvents() []Event {
	pending := p.events
	p.events = nil
	return pending
}
