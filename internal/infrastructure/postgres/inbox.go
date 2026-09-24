package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrInbox = errors.New("postgres: inbox")

const claimEvent = `
INSERT INTO inbox (event_id, consumed_at) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING`

type InboxStore struct {
	storage *Storage
}

func NewInboxStore(storage *Storage) *InboxStore {
	return &InboxStore{storage: storage}
}

// Claim reports whether this event is being handled for the first time. The decision is the
// insert itself: two consumers handed the same redelivered event both try to write the same
// key, and only one of them can. It has to run in the transaction that does the work, or
// the claim would survive a rollback and the event would be dropped instead of retried.
func (s *InboxStore) Claim(ctx context.Context, eventID string, at time.Time) (bool, error) {
	if _, inside := transaction(ctx); !inside {
		return false, fmt.Errorf("postgres.Claim inbox: %w", ErrOutsideTransaction)
	}

	claimed, err := s.storage.conn(ctx).Exec(ctx, claimEvent, eventID, at.UTC())
	if err != nil {
		return false, fmt.Errorf("postgres.Claim inbox %s: %w", eventID, errors.Join(ErrInbox, err))
	}
	return claimed.RowsAffected() == 1, nil
}
