package payments

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

var (
	ErrIdempotencyKeyRequired = errors.New("payments: idempotency key is required")
	ErrIdempotencyKeyReused   = errors.New("payments: the idempotency key was used for a different amount")
)

// paymentStore stores the aggregate and the events it is holding in one transaction, and
// refuses to store the same idempotency key twice: the key belongs to the caller's protocol,
// not to the payment, so it travels beside the aggregate rather than inside it. A replayed
// key that asks for a different amount is ErrIdempotencyKeyReused, never the earlier
// payment: a caller told its 5 000.00 succeeded when 19.99 was authorised is worse off than
// one told to look again.
type paymentStore interface {
	CreateOrGet(ctx context.Context, authorized *payment.Payment, idempotencyKey string) (payment.ID, bool, error)
}

type txManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type Clock func() time.Time

type IDs func() payment.ID

type AuthorizePayment struct {
	payments paymentStore
	tx       txManager
	now      Clock
	newID    IDs
}

func NewAuthorizePayment(payments paymentStore, tx txManager, now Clock, newID IDs) *AuthorizePayment {
	return &AuthorizePayment{payments: payments, tx: tx, now: now, newID: newID}
}

type AuthorizeCommand struct {
	MerchantID     string
	AmountMinor    int64
	Currency       string
	IdempotencyKey string
}

type AuthorizeResult struct {
	PaymentID string
	Stored    bool
}

func (uc *AuthorizePayment) Execute(ctx context.Context, cmd AuthorizeCommand) (AuthorizeResult, error) {
	if cmd.IdempotencyKey == "" {
		return AuthorizeResult{}, ErrIdempotencyKeyRequired
	}

	authorized, err := authorize(cmd, uc.now(), uc.newID())
	if err != nil {
		return AuthorizeResult{}, err
	}

	var stored AuthorizeResult
	err = uc.tx.WithinTx(ctx, func(ctx context.Context) error {
		id, created, err := uc.payments.CreateOrGet(ctx, authorized, cmd.IdempotencyKey)
		if err != nil {
			return err
		}
		stored = AuthorizeResult{PaymentID: id.String(), Stored: created}
		return nil
	})
	if err != nil {
		return AuthorizeResult{}, fmt.Errorf("payments: authorize for merchant %s: %w", cmd.MerchantID, err)
	}
	return stored, nil
}

func authorize(cmd AuthorizeCommand, at time.Time, id payment.ID) (*payment.Payment, error) {
	merchant, err := payment.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return nil, err
	}
	currency, err := payment.ParseCurrency(cmd.Currency)
	if err != nil {
		return nil, err
	}
	amount, err := payment.NewMoney(cmd.AmountMinor, currency)
	if err != nil {
		return nil, err
	}
	return payment.Authorize(id, merchant, amount, at)
}
