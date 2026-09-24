package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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

type TotalsStore struct {
	storage *Storage
}

func NewTotalsStore(storage *Storage) *TotalsStore {
	return &TotalsStore{storage: storage}
}

const readTotal = `SELECT authorized_minor FROM merchant_totals WHERE merchant_id = $1`

// Total reads outside a transaction on purpose: it answers a question, it does not take
// part in one, and a reader that took a row lock would stand in the consumer's way.
func (s *TotalsStore) Total(ctx context.Context, merchantID string) (int64, error) {
	var total int64
	err := s.storage.conn(ctx).QueryRow(ctx, readTotal, merchantID).Scan(&total)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("postgres.Total %s: %w", merchantID, errors.Join(ErrTotals, err))
	}
	return total, nil
}

func (s *TotalsStore) Add(ctx context.Context, merchantID string, minor int64, at time.Time) error {
	if _, inside := transaction(ctx); !inside {
		return fmt.Errorf("postgres.Add total: %w", ErrOutsideTransaction)
	}

	if _, err := s.storage.conn(ctx).Exec(ctx, addToTotal, merchantID, minor, at.UTC()); err != nil {
		return fmt.Errorf("postgres.Add total %s: %w", merchantID, errors.Join(ErrTotals, err))
	}
	return nil
}
