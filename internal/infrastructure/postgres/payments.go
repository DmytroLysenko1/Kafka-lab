package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

const insertPayment = `
INSERT INTO payments (id, merchant_id, amount_minor, currency, status, idempotency_key, authorized_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (merchant_id, idempotency_key) DO NOTHING`

const selectPaymentByKey = `
SELECT id, amount_minor, currency FROM payments WHERE merchant_id = $1 AND idempotency_key = $2`

const insertOutbox = `
INSERT INTO outbox (aggregate_id, event_type, payload, occurred_at) VALUES ($1, $2, $3, $4)`

type PaymentStore struct {
	storage *Storage
}

func NewPaymentStore(storage *Storage) *PaymentStore {
	return &PaymentStore{
		storage: storage,
	}
}

// CreateOrGet decides insert-or-replay inside the insert itself: a SELECT followed by an
// INSERT would let two concurrent retries of one request both find nothing and both charge
// the merchant. The unique key on (merchant_id, idempotency_key) is what makes it one row.
// It refuses to run outside a transaction, because the payment and the event it publishes
// would then land in two of them, and a crash between the two would tell consumers about a
// payment the database never kept.
func (s *PaymentStore) CreateOrGet(ctx context.Context, authorized *payment.Payment, idempotencyKey string) (payment.ID, bool, error) {
	if _, inside := transaction(ctx); !inside {
		return payment.ID{}, false, fmt.Errorf("postgres.CreateOrGet: %w", ErrOutsideTransaction)
	}

	inserted, err := s.storage.conn(ctx).Exec(ctx, insertPayment,
		authorized.ID().String(),
		authorized.Merchant().String(),
		authorized.Amount().Minor(),
		authorized.Amount().Currency().String(),
		authorized.Status().String(),
		idempotencyKey,
		authorized.AuthorizedAt(),
	)
	if err != nil {
		return payment.ID{}, false, fmt.Errorf("postgres.CreateOrGet %s: %w", authorized.ID(), errors.Join(payment.ErrStorage, err))
	}
	if inserted.RowsAffected() == 0 {
		return s.storedUnder(ctx, authorized, idempotencyKey)
	}
	if err := s.appendEvents(ctx, authorized); err != nil {
		return payment.ID{}, false, err
	}
	return authorized.ID(), true, nil
}

// storedUnder reads the payment the key already bought and lets the payment itself say
// whether the replay asks for the same thing.
func (s *PaymentStore) storedUnder(ctx context.Context, requested *payment.Payment, idempotencyKey string) (payment.ID, bool, error) {
	merchant := requested.Merchant()

	var (
		stored   string
		minor    int64
		currency string
	)
	row := s.storage.conn(ctx).QueryRow(ctx, selectPaymentByKey, merchant.String(), idempotencyKey)
	if err := row.Scan(&stored, &minor, &currency); err != nil {
		return payment.ID{}, false, fmt.Errorf("postgres.CreateOrGet replay %s: %w", merchant, errors.Join(payment.ErrStorage, err))
	}

	id, err := payment.ParseID(stored)
	if err != nil {
		return payment.ID{}, false, fmt.Errorf("postgres.CreateOrGet replay %s: %w", merchant, errors.Join(payment.ErrStorage, err))
	}
	amount, err := money(minor, currency)
	if err != nil {
		return payment.ID{}, false, fmt.Errorf("postgres.CreateOrGet replay %s: %w", merchant, errors.Join(payment.ErrStorage, err))
	}
	if !requested.IsFor(amount) {
		return payment.ID{}, false, fmt.Errorf("postgres.CreateOrGet replay %s: %w", merchant, payments.ErrIdempotencyKeyReused)
	}
	return id, false, nil
}

func money(minor int64, currency string) (payment.Money, error) {
	code, err := payment.ParseCurrency(currency)
	if err != nil {
		return payment.Money{}, err
	}
	return payment.NewMoney(minor, code)
}

func (s *PaymentStore) appendEvents(ctx context.Context, aggregate *payment.Payment) error {
	conn := s.storage.conn(ctx)
	for _, event := range aggregate.PullEvents() {
		row, err := encodeEvent(event)
		if err != nil {
			return fmt.Errorf("postgres.CreateOrGet outbox: %w", errors.Join(payment.ErrStorage, err))
		}
		if _, err := conn.Exec(ctx, insertOutbox, row.aggregateID, row.eventType, row.payload, row.occurredAt); err != nil {
			return fmt.Errorf("postgres.CreateOrGet outbox %s: %w", row.eventType, errors.Join(payment.ErrStorage, err))
		}
	}
	return nil
}
