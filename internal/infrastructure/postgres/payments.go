package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

const insertPayment = `
INSERT INTO payments (id, merchant_id, amount_minor, currency, status, idempotency_key, authorized_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (merchant_id, idempotency_key) DO NOTHING`

const selectPaymentByKey = `
SELECT id, amount_minor, currency, status, authorized_at, captured_at, refunded_at
FROM payments WHERE merchant_id = $1 AND idempotency_key = $2`

// The merchant is part of the key, not a check made after the read: a payment of another
// merchant is not found, the same as one that does not exist, so its id says nothing about it.
// The row stays locked until the transaction ends, so two changes of one payment run one
// after the other and the second sees what the first did.
const selectPaymentForUpdate = `
SELECT id, amount_minor, currency, status, authorized_at, captured_at, refunded_at
FROM payments WHERE id = $1 AND merchant_id = $2
FOR UPDATE`

// The status the change was decided from is part of the condition: the write applies only to
// the payment the decision was made about.
const updatePaymentStatus = `
UPDATE payments SET status = $3, captured_at = $4, refunded_at = $5
WHERE id = $1 AND merchant_id = $2 AND status = $6`

const insertOutbox = `
INSERT INTO outbox (aggregate_id, event_type, payload, occurred_at, trace_context) VALUES ($1, $2, $3, $4, $5)`

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
func (s *PaymentStore) CreateOrGet(ctx context.Context, authorized *payment.Payment, idempotencyKey payments.IdempotencyKey) (*payment.Payment, bool, error) {
	if _, inside := transaction(ctx); !inside {
		return nil, false, fmt.Errorf("postgres.CreateOrGet: %w", ErrOutsideTransaction)
	}
	status, err := storedStatus(authorized.Status())
	if err != nil {
		return nil, false, fmt.Errorf("postgres.CreateOrGet %s: %w", authorized.ID(), errors.Join(payment.ErrCorrupt, err))
	}

	inserted, err := s.storage.conn(ctx).Exec(ctx, insertPayment,
		authorized.ID().String(),
		authorized.Merchant().String(),
		authorized.Amount().Minor(),
		authorized.Amount().Currency().String(),
		status,
		idempotencyKey.String(),
		authorized.History().AuthorizedAt(),
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
func (s *PaymentStore) storedUnder(ctx context.Context, merchant payment.MerchantID, idempotencyKey payments.IdempotencyKey) (*payment.Payment, error) {
	var row storedPayment
	if err := row.scan(s.storage.conn(ctx).QueryRow(ctx, selectPaymentByKey, merchant.String(), idempotencyKey.String())); err != nil {
		return nil, fmt.Errorf("postgres.CreateOrGet replay %s: %w", merchant, errors.Join(payment.ErrStorage, err))
	}
	return rebuilt(&row, merchant)
}

// FindForMerchantForUpdate reads a payment to change it, and locks it until the transaction
// that reads it ends — outside one the lock would be released before the change it protects.
func (s *PaymentStore) FindForMerchantForUpdate(ctx context.Context, id payment.ID, merchant payment.MerchantID) (*payment.Payment, error) {
	if _, inside := transaction(ctx); !inside {
		return nil, fmt.Errorf("postgres.FindForMerchantForUpdate: %w", ErrOutsideTransaction)
	}
	var row storedPayment
	err := row.scan(s.storage.conn(ctx).QueryRow(ctx, selectPaymentForUpdate, id.String(), merchant.String()))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("postgres.FindForMerchantForUpdate %s: %w", id, payment.ErrNotFound)
	case err != nil:
		return nil, fmt.Errorf("postgres.FindForMerchantForUpdate %s: %w", id, errors.Join(payment.ErrStorage, err))
	}
	return rebuilt(&row, merchant)
}

// Save writes a change decided from the status the payment had when it was read, together with
// the events the change raised, in the transaction that read it.
func (s *PaymentStore) Save(ctx context.Context, changed *payment.Payment, from payment.Status) error {
	if _, inside := transaction(ctx); !inside {
		return fmt.Errorf("postgres.Save: %w", ErrOutsideTransaction)
	}
	status, err := storedStatus(changed.Status())
	if err != nil {
		return fmt.Errorf("postgres.Save %s: %w", changed.ID(), errors.Join(payment.ErrCorrupt, err))
	}
	previous, err := storedStatus(from)
	if err != nil {
		return fmt.Errorf("postgres.Save %s: %w", changed.ID(), errors.Join(payment.ErrCorrupt, err))
	}
	history := changed.History()
	updated, err := s.storage.conn(ctx).Exec(ctx, updatePaymentStatus,
		changed.ID().String(),
		changed.Merchant().String(),
		status,
		nullableTime(history.CapturedAt()),
		nullableTime(history.RefundedAt()),
		previous,
	)
	if err != nil {
		return fmt.Errorf("postgres.Save %s: %w", changed.ID(), errors.Join(payment.ErrStorage, err))
	}
	if updated.RowsAffected() == 0 {
		return fmt.Errorf("postgres.Save %s from %s: %w", changed.ID(), from, payment.ErrConcurrentModification)
	}
	return s.appendEvents(ctx, changed)
}

// rebuilt is a row the domain refuses to rebuild — a status this version does not know, say,
// written by a newer one — reported as corrupt rather than as a storage failure: a retry reads
// the same row. ErrCorrupt answers 500, where ErrStorage would tell the caller to repeat a
// request that can never succeed.
func rebuilt(row *storedPayment, merchant payment.MerchantID) (*payment.Payment, error) {
	stored, err := row.toDomain(merchant)
	if err != nil {
		return nil, fmt.Errorf("postgres.payment %s of %s: %w", row.id, merchant, errors.Join(payment.ErrCorrupt, err))
	}
	return stored, nil
}

// storedPayment is a payments row as it comes back from the table. A status not reached yet
// has no time: NULL, read as nil.
type storedPayment struct {
	id           string
	minor        int64
	currency     string
	status       string
	authorizedAt time.Time
	capturedAt   *time.Time
	refundedAt   *time.Time
}

func (r *storedPayment) scan(row pgx.Row) error {
	return row.Scan(&r.id, &r.minor, &r.currency, &r.status, &r.authorizedAt, &r.capturedAt, &r.refundedAt)
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
	status, err := parseStoredStatus(r.status)
	if err != nil {
		return nil, err
	}
	return payment.Reconstitute(id, merchant, amount, status, payment.NewHistory(r.authorizedAt, timeOrZero(r.capturedAt), timeOrZero(r.refundedAt))), nil
}

// traceContext is the trace the writing transaction runs in, stored with the event so the
// relay can publish it in that trace; nil — NULL — when it runs in none.
func traceContext(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return carrier
}

func nullableTime(at time.Time) *time.Time {
	if at.IsZero() {
		return nil
	}
	return &at
}

func timeOrZero(at *time.Time) time.Time {
	if at == nil {
		return time.Time{}
	}
	return *at
}

func (s *PaymentStore) appendEvents(ctx context.Context, aggregate *payment.Payment) error {
	conn := s.storage.conn(ctx)
	for _, event := range aggregate.PullEvents() {
		row, err := encodeEvent(event)
		if err != nil {
			return fmt.Errorf("postgres.appendEvents: %w", errors.Join(payment.ErrCorrupt, err))
		}
		if _, err := conn.Exec(ctx, insertOutbox, row.aggregateID, row.eventType, row.payload, row.occurredAt, traceContext(ctx)); err != nil {
			return fmt.Errorf("postgres.appendEvents %s: %w", row.eventType, errors.Join(payment.ErrStorage, err))
		}
	}
	return nil
}
