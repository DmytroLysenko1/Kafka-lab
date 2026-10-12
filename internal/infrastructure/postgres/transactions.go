package postgres

import (
	"context"
	"errors"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

// Transactions is the transaction boundary as one context sees it: the same transactions as
// Storage, with their failures said in that context's words, so no use case ever has to know a
// Postgres sentinel. A transaction that never started, or that the server rolled back at
// commit, was not applied; a commit whose outcome the server never reported may have been.
type Transactions struct {
	storage    *Storage
	notApplied error
	unknown    error
}

// NewPaymentTransactions keeps the two apart: a payment that may already be kept is answered
// differently from one that certainly was not — a retry with the same idempotency key is what
// finds out which.
func NewPaymentTransactions(storage *Storage) *Transactions {
	return &Transactions{
		storage:    storage,
		notApplied: payment.ErrStorage,
		unknown:    payment.ErrOutcomeUnknown,
	}
}

// NewMerchantTransactions says both as a storage failure: the record stays where it is and is
// handled again, and the inbox drops it if the commit that went unanswered had in fact counted it.
func NewMerchantTransactions(storage *Storage) *Transactions {
	return &Transactions{
		storage:    storage,
		notApplied: merchant.ErrStorage,
		unknown:    merchant.ErrStorage,
	}
}

// NewOutboxTransactions says both as a storage failure: rows whose marking went unanswered are
// published again by the next sweep, and the consumer's inbox drops the second copy.
func NewOutboxTransactions(storage *Storage) *Transactions {
	return &Transactions{
		storage:    storage,
		notApplied: outbox.ErrStorage,
		unknown:    outbox.ErrStorage,
	}
}

func (t *Transactions) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return t.inContextTerms(t.storage.WithinTx(ctx, fn))
}

func (t *Transactions) inContextTerms(err error) error {
	switch {
	case errors.Is(err, ErrCommitUnknown):
		return errors.Join(t.unknown, err)
	case errors.Is(err, ErrNotApplied):
		return errors.Join(t.notApplied, err)
	}
	return err
}
