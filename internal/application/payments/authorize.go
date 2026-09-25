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
// not to the payment, so it travels beside the aggregate rather than inside it. When the key
// is taken it returns the payment that key already bought, and reports it as not created;
// whether the replay may stand is the payment's decision, not the store's.
type paymentStore interface {
	CreateOrGet(ctx context.Context, authorized *payment.Payment, idempotencyKey string) (*payment.Payment, bool, error)
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
	return &AuthorizePayment{
		payments: payments,
		tx:       tx,
		now:      now,
		newID:    newID,
	}
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

	var result AuthorizeResult
	err = uc.tx.WithinTx(ctx, func(ctx context.Context) error {
		var err error
		result, err = uc.storeOnce(ctx, authorized, cmd.IdempotencyKey)
		return err
	})
	if err != nil {
		return AuthorizeResult{}, fmt.Errorf("payments: authorize for merchant %s: %w", cmd.MerchantID, err)
	}
	return result, nil
}

// storeOnce answers with the payment the key bought. A replayed key that asks for a
// different payment is ErrIdempotencyKeyReused, never the earlier payment: a caller told its
// 5 000.00 succeeded when 19.99 was authorised is worse off than one told to look again.
func (uc *AuthorizePayment) storeOnce(ctx context.Context, authorized *payment.Payment, idempotencyKey string) (AuthorizeResult, error) {
	stored, created, err := uc.payments.CreateOrGet(ctx, authorized, idempotencyKey)
	if err != nil {
		return AuthorizeResult{}, err
	}
	if !created {
		if err := authorized.Replays(stored); err != nil {
			return AuthorizeResult{}, fmt.Errorf("%w: %w", ErrIdempotencyKeyReused, err)
		}
	}
	return AuthorizeResult{
		PaymentID: stored.ID().String(),
		Stored:    created,
	}, nil
}

func authorize(cmd AuthorizeCommand, at time.Time, id payment.ID) (*payment.Payment, error) {
	merchant, err := payment.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return nil, err
	}
	amount, err := payment.ParseMoney(cmd.AmountMinor, cmd.Currency)
	if err != nil {
		return nil, err
	}
	return payment.Authorize(id, merchant, amount, at)
}
