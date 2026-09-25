package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

const insertPayment = `
INSERT INTO payments (id, merchant_id, amount_minor, currency, status, idempotency_key, authorized_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (merchant_id, idempotency_key) DO NOTHING`

const selectPaymentByKey = `
SELECT id, amount_minor, currency, status, authorized_at
FROM payments WHERE merchant_id = $1 AND idempotency_key = $2`

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
// payment the database never kept. When the key is taken it returns the payment stored
// under it; whether the replay may stand is for the payment to say, not for the store.
func (s *PaymentStore) CreateOrGet(ctx context.Context, authorized *payment.Payment, idempotencyKey string) (*payment.Payment, bool, error) {
	if _, inside := transaction(ctx); !inside {
		return nil, false, fmt.Errorf("postgres.CreateOrGet: %w", ErrOutsideTransaction)
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
		return nil, false, fmt.Errorf("postgres.CreateOrGet %s: %w", authorized.ID(), errors.Join(payment.ErrStorage, err))
	}
	if inserted.RowsAffected() == 0 {
		stored, err := s.storedUnder(ctx, authorized.Merchant(), idempotencyKey)
		return stored, false, err
	}
	if err := s.appendEvents(ctx, authorized); err != nil {
		return nil, false, err
	}
	return authorized, true, nil
}

// storedUnder rebuilds the payment the key already bought, as it was stored. A row that
// does not make a valid payment is storage failure: the CHECK constraints should have
// made it impossible, and comparing a replay against it would compare against nonsense.
func (s *PaymentStore) storedUnder(ctx context.Context, merchant payment.MerchantID, idempotencyKey string) (*payment.Payment, error) {
	var row storedPayment
	err := s.storage.conn(ctx).QueryRow(ctx, selectPaymentByKey, merchant.String(), idempotencyKey).
		Scan(&row.id, &row.minor, &row.currency, &row.status, &row.authorizedAt)
	if err != nil {
		return nil, fmt.Errorf("postgres.CreateOrGet replay %s: %w", merchant, errors.Join(payment.ErrStorage, err))
	}
	stored, err := row.toDomain(merchant)
	if err != nil {
		return nil, fmt.Errorf("postgres.CreateOrGet replay %s: %w", merchant, errors.Join(payment.ErrStorage, err))
	}
	return stored, nil
}

// storedPayment is a payments row as it comes back from the table.
type storedPayment struct {
	id           string
	minor        int64
	currency     string
	status       string
	authorizedAt time.Time
}

func (r *storedPayment) toDomain(merchant payment.MerchantID) (*payment.Payment, error) {
	id, err := payment.ParseID(r.id)
	if err != nil {
		return nil, err
	}
	amount, err := payment.ParseMoney(r.minor, r.currency)
	if err != nil {
		return nil, err
	}
	status, err := payment.ParseStatus(r.status)
	if err != nil {
		return nil, err
	}
	return payment.Reconstitute(id, merchant, amount, status, r.authorizedAt), nil
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
