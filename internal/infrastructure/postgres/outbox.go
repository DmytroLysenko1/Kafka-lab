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
)

// MaxClaim is the most rows one claim may lock. The rows stay locked while their batch is
// published — up to the sweep's 30 s against a sick broker — and go out as one produce
// request, so it bounds both the lock set and the request; ten times the relay's batch of
// 100, a ceiling rather than a measured size. The relay checks its configured batch against it
// at start (cmd/outbox-relay), so a batch past it fails there instead of on every sweep.
const MaxClaim = 1000

const claimUnpublished = `
SELECT id, aggregate_id, event_type, payload, occurred_at, trace_context
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
	if limit <= 0 || limit > MaxClaim {
		return nil, fmt.Errorf("postgres.Claim %d: %w", limit, ErrClaimLimit)
	}
	if _, inside := transaction(ctx); !inside {
		return nil, fmt.Errorf("postgres.Claim: %w", ErrOutsideTransaction)
	}

	rows, err := s.storage.conn(ctx).Query(ctx, claimUnpublished, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres.Claim: %w", errors.Join(outbox.ErrStorage, err))
	}
	defer rows.Close()

	claimed := make([]outbox.Record, 0, limit)
	for rows.Next() {
		var record outbox.Record
		if err := rows.Scan(&record.ID, &record.AggregateID, &record.EventType, &record.Payload, &record.OccurredAt, &record.TraceContext); err != nil {
			return nil, fmt.Errorf("postgres.Claim scan: %w", errors.Join(outbox.ErrStorage, err))
		}
		claimed = append(claimed, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres.Claim: %w", errors.Join(outbox.ErrStorage, err))
	}
	return claimed, nil
}

// Backlog is what an operator watches while a broker is away: it grows while nothing can
// be published and falls back once the relay drains. It reads outside a transaction — a
// count that took locks would stand in the way of the relay it is reporting on.
func (s *OutboxStore) Backlog(ctx context.Context) (int, error) {
	var waiting int
	if err := s.storage.conn(ctx).QueryRow(ctx, countBacklog).Scan(&waiting); err != nil {
		return 0, fmt.Errorf("postgres.Backlog: %w", errors.Join(outbox.ErrStorage, err))
	}
	return waiting, nil
}

// MarkPublished runs in the transaction that claimed the rows, so they are still locked to
// this relay; "published_at IS NULL" only keeps a timestamp, once set, from ever moving. A
// relay that published and died before this committed leaves the rows unpublished, and the
// next sweep publishes them again (application/outbox/relay.go): the consumer's inbox is what
// makes that second copy harmless.
func (s *OutboxStore) MarkPublished(ctx context.Context, ids []int64, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.storage.conn(ctx).Exec(ctx, markPublished, ids, at.UTC()); err != nil {
		return fmt.Errorf("postgres.MarkPublished %d records: %w", len(ids), errors.Join(outbox.ErrStorage, err))
	}
	return nil
}
