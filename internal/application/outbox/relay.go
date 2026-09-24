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

type claimStore interface {
	Claim(ctx context.Context, limit int) ([]Record, error)
	MarkPublished(ctx context.Context, ids []int64, at time.Time) error
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
}

func NewRelay(outbox claimStore, publisher eventPublisher, tx txManager, now Clock, batch int, logger *slog.Logger) *Relay {
	return &Relay{outbox: outbox, publisher: publisher, tx: tx, now: now, batch: batch, logger: logger}
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

		ids := lo.Map(claimed, func(record Record, _ int) int64 { return record.ID })
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
	published, err := r.Sweep(ctx)
	switch {
	case err != nil && ctx.Err() != nil:
		return
	case err != nil:
		r.logger.ErrorContext(ctx, "outbox sweep failed", "error", err)
	case published > 0:
		r.logger.InfoContext(ctx, "outbox published", "records", published)
	}
}
