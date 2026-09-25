package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
)

var (
	ErrOutsideTransaction = errors.New("postgres: the write needs a transaction")
	ErrClaimLimit         = errors.New("postgres: outbox claim needs a limit between 1 and the batch ceiling")
	ErrOutbox             = errors.New("postgres: outbox")
)

const maxClaim = 1000

const claimUnpublished = `
SELECT id, aggregate_id, event_type, payload, occurred_at
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED`

const countBacklog = `
SELECT count(*) FROM outbox WHERE published_at IS NULL`

const markPublished = `
UPDATE outbox SET published_at = $2 WHERE id = ANY($1) AND published_at IS NULL`

type OutboxStore struct {
	storage *Storage
}

func NewOutboxStore(storage *Storage) *OutboxStore {
	return &OutboxStore{
		storage: storage,
	}
}

// Claim hands each row to exactly one relay: SKIP LOCKED steps over rows another relay is
// already holding, and the rows stay locked until the surrounding transaction ends — which
// is why claiming outside one is refused rather than quietly handing everyone the same work.
func (s *OutboxStore) Claim(ctx context.Context, limit int) ([]outbox.Record, error) {
	if limit <= 0 || limit > maxClaim {
		return nil, fmt.Errorf("postgres.Claim %d: %w", limit, ErrClaimLimit)
	}
	if _, inside := transaction(ctx); !inside {
		return nil, fmt.Errorf("postgres.Claim: %w", ErrOutsideTransaction)
	}

	rows, err := s.storage.conn(ctx).Query(ctx, claimUnpublished, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres.Claim: %w", errors.Join(ErrOutbox, err))
	}
	defer rows.Close()

	claimed := make([]outbox.Record, 0, limit)
	for rows.Next() {
		var record outbox.Record
		if err := rows.Scan(&record.ID, &record.AggregateID, &record.EventType, &record.Payload, &record.OccurredAt); err != nil {
			return nil, fmt.Errorf("postgres.Claim scan: %w", errors.Join(ErrOutbox, err))
		}
		claimed = append(claimed, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres.Claim: %w", errors.Join(ErrOutbox, err))
	}
	return claimed, nil
}

// Backlog is what an operator watches while a broker is away: it grows while nothing can
// be published and falls back once the relay drains. It reads outside a transaction — a
// count that took locks would stand in the way of the relay it is reporting on.
func (s *OutboxStore) Backlog(ctx context.Context) (int, error) {
	var waiting int
	if err := s.storage.conn(ctx).QueryRow(ctx, countBacklog).Scan(&waiting); err != nil {
		return 0, fmt.Errorf("postgres.Backlog: %w", errors.Join(ErrOutbox, err))
	}
	return waiting, nil
}

// MarkPublished is written to survive a relay that published and then died before it could
// record that: the second attempt matches no rows and is a no-op, never a second publish.
func (s *OutboxStore) MarkPublished(ctx context.Context, ids []int64, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.storage.conn(ctx).Exec(ctx, markPublished, ids, at.UTC()); err != nil {
		return fmt.Errorf("postgres.MarkPublished %d records: %w", len(ids), errors.Join(ErrOutbox, err))
	}
	return nil
}
