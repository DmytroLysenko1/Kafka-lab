package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"
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

func (s *TotalsStore) Add(ctx context.Context, merchantID string, minor int64, at time.Time) error {
	if _, inside := transaction(ctx); !inside {
		return fmt.Errorf("postgres.Add total: %w", ErrOutsideTransaction)
	}

	if _, err := s.storage.conn(ctx).Exec(ctx, addToTotal, merchantID, minor, at.UTC()); err != nil {
		return fmt.Errorf("postgres.Add total %s: %w", merchantID, errors.Join(ErrTotals, err))
	}
	return nil
}
