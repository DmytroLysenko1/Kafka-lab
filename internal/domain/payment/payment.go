package payment

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrMerchantRequired  = errors.New("payment: merchant id is required")
	ErrMerchantTooLong   = errors.New("payment: merchant id is longer than allowed")
	ErrIDFormat          = errors.New("payment: id is not a uuid")
	ErrAmountNotPositive = errors.New("payment: an authorised payment is for a positive amount")
	ErrStorage           = errors.New("payment: storage failure")
	ErrStatusUnknown     = errors.New("payment: status is not one a payment can be in")
	ErrNotTheSamePayment = errors.New("payment: a replay asks for a different payment than the one it repeats")
)

const merchantIDMaxLength = 64

type ID struct {
	value uuid.UUID
}

func NewID() ID { return ID{value: uuid.New()} }

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
	}
	return MerchantID{
		value: value,
	}, nil
}

func (m MerchantID) String() string {
	return m.value
}

type Status uint8

const (
	StatusUnknown Status = iota
	StatusAuthorized
)

const statusAuthorized = "authorized"

func (s Status) String() string {
	if s == StatusAuthorized {
		return statusAuthorized
	}
	return "unknown"
}

// ParseStatus reads a status back from storage. Only a status a payment can actually be in
// is accepted: an "unknown" row is a corrupted one, not a payment in an unknown state.
func ParseStatus(value string) (Status, error) {
	if value != statusAuthorized {
		return StatusUnknown, ErrStatusUnknown
	}
	return StatusAuthorized, nil
}

type Payment struct {
	id           ID
	merchant     MerchantID
	amount       Money
	status       Status
	authorizedAt time.Time
	events       []Event
}

// Authorize is the only way a Payment comes into existence, so an amount of zero cannot be
// authorised even though Money itself allows it: a zero-amount authorisation reserves
// nothing and would still emit an event downstream consumers would act on.
func Authorize(id ID, merchant MerchantID, amount Money, at time.Time) (*Payment, error) {
	if !amount.IsPositive() {
		return nil, ErrAmountNotPositive
	}

	authorized := &Payment{
		id:           id,
		merchant:     merchant,
		amount:       amount,
		status:       StatusAuthorized,
		authorizedAt: at.UTC(),
	}
	authorized.events = append(authorized.events, &Authorized{
		paymentID:  id,
		merchant:   merchant,
		amount:     amount,
		occurredAt: authorized.authorizedAt,
	})
	return authorized, nil
}

// Reconstitute rebuilds a payment that was authorised earlier and stored. It emits nothing:
// the payment's events were published when it came into existence, and rebuilding it to
// compare a replay against must not announce it a second time.
func Reconstitute(id ID, merchant MerchantID, amount Money, status Status, authorizedAt time.Time) *Payment {
	return &Payment{
		id:           id,
		merchant:     merchant,
		amount:       amount,
		status:       status,
		authorizedAt: authorizedAt.UTC(),
	}
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

func (p *Payment) AuthorizedAt() time.Time {
	return p.authorizedAt
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
