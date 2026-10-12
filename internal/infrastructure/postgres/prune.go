package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/retention"
)

// Each statement deletes one batch and commits on its own: no transaction spans the run, so
// no lock is held longer than one batch takes. The subquery is what bounds the batch — DELETE
// has no LIMIT of its own in Postgres.
const (
	pruneInbox = `
DELETE FROM inbox
WHERE event_id IN (SELECT event_id FROM inbox WHERE consumed_at < $1 ORDER BY consumed_at LIMIT $2)`

	// published_at < $1 is false for a NULL, so an event still owed to Kafka is never
	// deleted, however old its row is.
	pruneOutbox = `
DELETE FROM outbox
WHERE id IN (SELECT id FROM outbox WHERE published_at < $1 ORDER BY id LIMIT $2)`
)

type PruneStore struct {
	storage *Storage
}

func NewPruneStore(storage *Storage) *PruneStore {
	return &PruneStore{
		storage: storage,
	}
}

func (s *PruneStore) PruneInbox(ctx context.Context, consumedBefore time.Time, limit int) (int, error) {
	return s.prune(ctx, "inbox", pruneInbox, consumedBefore, limit)
}

func (s *PruneStore) PruneOutbox(ctx context.Context, publishedBefore time.Time, limit int) (int, error) {
	return s.prune(ctx, "outbox", pruneOutbox, publishedBefore, limit)
}

func (s *PruneStore) prune(ctx context.Context, table, statement string, before time.Time, limit int) (int, error) {
	tag, err := s.storage.pool.Exec(ctx, statement, before.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("postgres.prune %s before %s: %w", table, before.UTC().Format(time.RFC3339), errors.Join(retention.ErrStorage, err))
	}
	return int(tag.RowsAffected()), nil
}
