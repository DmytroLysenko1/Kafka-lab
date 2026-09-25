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

var ErrTotals = errors.New("postgres: merchant totals")

// The addition happens in the database, not in this process: reading a total, adding to it
// in Go and writing it back would lose an increment every time two consumers of different
// partitions touched the same merchant at once.
const addToTotal = `
INSERT INTO merchant_totals (merchant_id, authorized_minor, updated_at)
VALUES ($1, $2, $3)
ON CONFLICT (merchant_id) DO UPDATE
SET authorized_minor = merchant_totals.authorized_minor + EXCLUDED.authorized_minor,
    updated_at = EXCLUDED.updated_at`

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
	return &TotalsStore{storage: storage}
}

const readTotal = `SELECT authorized_minor FROM merchant_totals WHERE merchant_id = $1`

// Total reads outside a transaction on purpose: it answers a question, it does not take
// part in one, and a reader that took a row lock would stand in the consumer's way.
func (s *TotalsStore) Total(ctx context.Context, id merchant.ID) (int64, error) {
	var total int64
	err := s.storage.conn(ctx).QueryRow(ctx, readTotal, id.String()).Scan(&total)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("postgres.Total %s: %w", id, errors.Join(ErrTotals, err))
	}
	return total, nil
}

func (s *TotalsStore) Add(ctx context.Context, authorization merchant.Authorization, at time.Time) error {
	if _, inside := transaction(ctx); !inside {
		return fmt.Errorf("postgres.Add total: %w", ErrOutsideTransaction)
	}

	conn := s.storage.conn(ctx)
	if _, err := conn.Exec(ctx, boundLockWait, strconv.FormatInt(lockWait.Milliseconds(), 10)); err != nil {
		return fmt.Errorf("postgres.Add total %s: lock wait: %w", authorization.Merchant(), errors.Join(ErrTotals, err))
	}
	if _, err := conn.Exec(ctx, addToTotal, authorization.Merchant().String(), authorization.Minor(), at.UTC()); err != nil {
		return fmt.Errorf("postgres.Add total %s: %w", authorization.Merchant(), translateAddFailure(err))
	}
	return nil
}

func translateAddFailure(err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == lockNotAvailable {
		return errors.Join(merchants.ErrContended, err)
	}
	return errors.Join(ErrTotals, err)
}
