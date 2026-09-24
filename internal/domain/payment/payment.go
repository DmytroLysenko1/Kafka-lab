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
	return ID{value: parsed}, nil
}

func (id ID) String() string { return id.value.String() }

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
	return MerchantID{value: value}, nil
}

func (m MerchantID) String() string { return m.value }

type Status uint8

const (
	StatusUnknown Status = iota
	StatusAuthorized
)

func (s Status) String() string {
	if s == StatusAuthorized {
		return "authorized"
	}
	return "unknown"
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
	if amount.Minor() <= 0 {
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

func (p *Payment) ID() ID { return p.id }

func (p *Payment) Merchant() MerchantID { return p.merchant }

func (p *Payment) Amount() Money { return p.amount }

func (p *Payment) Status() Status { return p.status }

func (p *Payment) AuthorizedAt() time.Time { return p.authorizedAt }

// PullEvents hands the pending events over and forgets them, so the repository that stores
// the aggregate is the only place that can publish them and can only do it once.
func (p *Payment) PullEvents() []Event {
	pending := p.events
	p.events = nil
	return pending
}
