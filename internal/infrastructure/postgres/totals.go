package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
)

// The addition happens in the database, not in this process: reading a total, adding to it
// in Go and writing it back would lose an increment every time two consumers of different
// partitions touched the same merchant at once. %[1]s is the column of the movement's kind,
// always one of sumColumns — never text that came off a topic.
const addToTotal = `
INSERT INTO merchant_currency_totals (merchant_id, currency, %[1]s, updated_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (merchant_id, currency) DO UPDATE
SET %[1]s = merchant_currency_totals.%[1]s + EXCLUDED.%[1]s,
    updated_at = EXCLUDED.updated_at`

// sumColumns is the one place a kind of movement is given its column.
func sumColumns() map[merchant.Kind]string {
	return map[merchant.Kind]string{
		merchant.KindAuthorized: "authorized_minor",
		merchant.KindCaptured:   "captured_minor",
		merchant.KindRefunded:   "refunded_minor",
	}
}

var errNoColumn = errors.New("postgres: the movement has no column to be counted in")

// boundLockWait applies to the rest of the transaction only. Without it a row held by a
// long transaction elsewhere — a report, a reconciliation — keeps the consumer waiting on
// it, and the consumer holds its whole partition while it waits.
const boundLockWait = `SELECT set_config('lock_timeout', $1, true)`

// lockWait is well inside the consumer's per-record budget, so a contended record is
// reported as contended rather than as a record that ran out of time.
const lockWait = 2 * time.Second

// lockNotAvailable is Postgres's answer when lock_timeout expires.
const lockNotAvailable = "55P03"

type TotalsStore struct {
	storage *Storage
}

func NewTotalsStore(storage *Storage) *TotalsStore {
	return &TotalsStore{
		storage: storage,
	}
}

const readTotals = `
SELECT currency, authorized_minor, captured_minor, refunded_minor FROM merchant_currency_totals
WHERE merchant_id = $1
ORDER BY currency`

// Totals reads outside a transaction on purpose: it answers a question, it does not take
// part in one, and a reader that took a row lock would stand in the consumer's way. A
// merchant with no rows has an empty list, not a nil one: the answer is "nothing yet".
func (s *TotalsStore) Totals(ctx context.Context, id merchant.ID) ([]merchant.Total, error) {
	rows, err := s.storage.conn(ctx).Query(ctx, readTotals, id.String())
	if err != nil {
		return nil, fmt.Errorf("postgres.Totals %s: %w", id, errors.Join(merchant.ErrStorage, err))
	}
	totals, err := pgx.AppendRows(make([]merchant.Total, 0), rows, scanTotal)
	if err != nil {
		return nil, fmt.Errorf("postgres.Totals %s: %w", id, errors.Join(merchant.ErrStorage, err))
	}
	return totals, nil
}

func scanTotal(row pgx.CollectableRow) (merchant.Total, error) {
	var (
		currency                       string
		authorized, captured, refunded int64
	)
	if err := row.Scan(&currency, &authorized, &captured, &refunded); err != nil {
		return merchant.Total{}, err
	}
	// A row NewTotal refuses would be a corrupt table rather than an outage, but the schema
	// admits none: the table's CHECKs hold currency to three capitals and every sum to ≥ 0,
	// exactly what NewTotal refuses (migrations 00003 and 00004).
	return merchant.NewTotal(currency, authorized, captured, refunded)
}

func (s *TotalsStore) Add(ctx context.Context, movement merchant.Movement, at time.Time) error {
	if _, inside := transaction(ctx); !inside {
		return fmt.Errorf("postgres.Add total: %w", ErrOutsideTransaction)
	}
	column, known := sumColumns()[movement.Kind()]
	if !known {
		return fmt.Errorf("postgres.Add total %s: %w", movement.Kind(), errors.Join(merchants.ErrUnprocessable, errNoColumn))
	}

	conn := s.storage.conn(ctx)
	if _, err := conn.Exec(ctx, boundLockWait, strconv.FormatInt(lockWait.Milliseconds(), 10)); err != nil {
		return fmt.Errorf("postgres.Add total %s: lock wait: %w", movement.Merchant(), errors.Join(merchant.ErrStorage, err))
	}
	amount := movement.Amount()
	if _, err := conn.Exec(ctx, fmt.Sprintf(addToTotal, column), movement.Merchant().String(), amount.Currency().String(), amount.Minor(), at.UTC()); err != nil {
		return fmt.Errorf("postgres.Add %s total %s: %w", movement.Kind(), movement.Merchant(), translateAddFailure(err))
	}
	return nil
}

// translateAddFailure sorts a failed add by what a retry would do: a lock wait may clear, so
// the record moves to a retry tier; a value the database refuses — a total past bigint's
// range — never will, so the record is refused and dead-lettered instead of stopping the
// consumer on it; anything else is the database itself, and the record stays where it is.
func translateAddFailure(err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == lockNotAvailable {
		return errors.Join(merchants.ErrContended, err)
	}
	if refusedByData(err) {
		return errors.Join(merchants.ErrUnprocessable, err)
	}
	return errors.Join(merchant.ErrStorage, err)
}
