// Package housekeeping keeps the service's bookkeeping tables from growing forever without
// forgetting anything that is still load-bearing.
package housekeeping

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/retention"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

var ErrBatch = errors.New("housekeeping: the delete batch must be positive")

type pruneStore interface {
	PruneInbox(ctx context.Context, consumedBefore time.Time, limit int) (int, error)
	PruneOutbox(ctx context.Context, publishedBefore time.Time, limit int) (int, error)
}

type Clock func() time.Time

type Result struct {
	Inbox  int
	Outbox int
}

type Prune struct {
	store    pruneStore
	now      Clock
	horizons retention.Horizons
	batch    int
}

func NewPrune(store pruneStore, now Clock, horizons retention.Horizons, batch int) (*Prune, error) {
	if batch <= 0 {
		return nil, ErrBatch
	}
	return &Prune{
		store:    store,
		now:      now,
		horizons: horizons,
		batch:    batch,
	}, nil
}

// Execute deletes in batches, each its own short statement, until a batch comes back short:
// one DELETE over a month of rows would hold its locks and bloat its transaction for as long
// as it ran. A run cut short by its deadline leaves the rest for the next run, which is safe —
// every batch only removes rows past their horizon.
func (p *Prune) Execute(ctx context.Context) (Result, error) {
	ctx, end := telemetry.Start(ctx, "housekeeping.Prune")
	result, err := p.execute(ctx)
	end(err)
	return result, err
}

func (p *Prune) execute(ctx context.Context) (Result, error) {
	now := p.now()
	inbox, err := p.drain(ctx, func(ctx context.Context, limit int) (int, error) {
		return p.store.PruneInbox(ctx, p.horizons.InboxCutoff(now), limit)
	})
	if err != nil {
		return Result{
			Inbox: inbox,
		}, fmt.Errorf("housekeeping: prune the inbox: %w", err)
	}
	outbox, err := p.drain(ctx, func(ctx context.Context, limit int) (int, error) {
		return p.store.PruneOutbox(ctx, p.horizons.OutboxCutoff(now), limit)
	})
	if err != nil {
		return Result{
			Inbox:  inbox,
			Outbox: outbox,
		}, fmt.Errorf("housekeeping: prune the outbox: %w", err)
	}
	return Result{
		Inbox:  inbox,
		Outbox: outbox,
	}, nil
}

func (p *Prune) drain(ctx context.Context, deleteBatch func(context.Context, int) (int, error)) (int, error) {
	total := 0
	for {
		deleted, err := deleteBatch(ctx, p.batch)
		total += deleted
		if err != nil || deleted < p.batch {
			return total, err
		}
	}
}
