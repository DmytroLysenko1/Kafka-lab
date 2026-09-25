package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/samber/lo"
)

var ErrInterval = errors.New("outbox: the relay needs a positive sweep interval")

const (
	// The publish inside a sweep is bounded by the publisher and the commit by the store,
	// each on its own timeout; this is the outer fence around the pair, so a database that
	// stops answering cannot hold the claimed rows and the loop with them. The commit runs
	// on a context that cannot be cancelled, so this deadline can never truncate it.
	sweepTimeout = 30 * time.Second
	// The backlog is a number for a dashboard, not work. It gets a short leash of its own:
	// a slow count must not delay the sweep that follows it.
	backlogTimeout = 2 * time.Second
)

type claimStore interface {
	Claim(ctx context.Context, limit int) ([]Record, error)
	MarkPublished(ctx context.Context, ids []int64, at time.Time) error
	Backlog(ctx context.Context) (int, error)
}

// observer is told what each sweep did, in the relay's own words. What those numbers are
// called once they leave here is the metrics adapter's business, not this package's. The
// backlog is reported separately because it can be unavailable on its own: a sweep that
// failed must still be counted as a failed sweep, and a backlog nobody could read must not
// be reported as a backlog of zero.
type observer interface {
	Swept(published int, took time.Duration, err error)
	Backlog(records int)
}

type eventPublisher interface {
	Publish(ctx context.Context, records []Record) error
}

type txManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type Clock func() time.Time

type Relay struct {
	outbox    claimStore
	publisher eventPublisher
	tx        txManager
	now       Clock
	batch     int
	logger    *slog.Logger
	observer  observer
}

func NewRelay(outbox claimStore, publisher eventPublisher, tx txManager, now Clock, batch int, logger *slog.Logger, watch observer) *Relay {
	return &Relay{
		outbox:    outbox,
		publisher: publisher,
		tx:        tx,
		now:       now,
		batch:     batch,
		logger:    logger,
		observer:  watch,
	}
}

// Sweep publishes one batch and reports how many records went out. The order is the whole
// point: the records are marked published only after the broker has acknowledged them, so
// a crash in between republishes an event rather than losing it. A consumer sees the
// duplicate; nobody sees a payment that was never announced.
func (r *Relay) Sweep(ctx context.Context) (int, error) {
	published := 0

	err := r.tx.WithinTx(ctx, func(ctx context.Context) error {
		claimed, err := r.outbox.Claim(ctx, r.batch)
		if err != nil {
			return err
		}
		if len(claimed) == 0 {
			return nil
		}
		if err := r.publisher.Publish(ctx, claimed); err != nil {
			return err
		}

		ids := lo.Map(claimed, func(record Record, _ int) int64 {
			return record.ID
		})
		if err := r.outbox.MarkPublished(ctx, ids, r.now()); err != nil {
			return err
		}
		published = len(claimed)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("outbox: sweep: %w", err)
	}
	return published, nil
}

// Run sweeps until the context is cancelled. A failed sweep is logged and retried on the
// next tick rather than ending the relay: the usual reason for one is a broker that will
// come back, and the records it did not publish are still in the table.
func (r *Relay) Run(ctx context.Context, every time.Duration) error {
	if every <= 0 {
		return fmt.Errorf("outbox: run every %s: %w", every, ErrInterval)
	}

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		r.sweepOnce(ctx)

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *Relay) sweepOnce(ctx context.Context) {
	sweeping, cancel := context.WithTimeout(ctx, sweepTimeout)
	defer cancel()

	started := r.now()
	published, err := r.Sweep(sweeping)
	r.observe(ctx, published, r.now().Sub(started), err)

	switch {
	case err != nil && ctx.Err() != nil:
		return
	case err != nil:
		r.logger.ErrorContext(ctx, "outbox sweep failed", "error", err)
	case published > 0:
		r.logger.InfoContext(ctx, "outbox published", "records", published)
	}
}

// observe reports the sweep first and the backlog second: the sweep is what happened and
// must be counted even when the database is too unwell to answer how much is waiting. A
// backlog that cannot be read leaves the last known one standing rather than reporting a
// zero, which an operator would read as "nothing is waiting" at the worst possible moment.
func (r *Relay) observe(ctx context.Context, published int, took time.Duration, sweepErr error) {
	r.observer.Swept(published, took, sweepErr)

	counting, cancel := context.WithTimeout(ctx, backlogTimeout)
	defer cancel()

	backlog, err := r.outbox.Backlog(counting)
	if err != nil {
		r.logger.WarnContext(ctx, "outbox backlog unavailable", "error", err)
		return
	}
	r.observer.Backlog(backlog)
}
