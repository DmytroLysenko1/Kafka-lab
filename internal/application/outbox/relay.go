package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

var (
	ErrInterval = errors.New("outbox: the relay needs a positive sweep interval")
	// ErrBatch guards Run's shortcut: with a batch of 0 every empty sweep would count as a
	// full one, and Run would sweep back to back forever without ever waiting for the tick.
	ErrBatch = errors.New("outbox: the relay needs a positive batch size")
	// ErrTimeout guards the loop: a zero sweep timeout would fail every sweep at once and a
	// zero backlog timeout would never let the gauge move.
	ErrTimeout = errors.New("outbox: the relay needs positive sweep and backlog timeouts")
	// ErrStorage is the outbox table failing: the sweep is retried on the next tick, and the
	// rows it could not claim or mark are still there.
	ErrStorage = errors.New("outbox: storage failure")
)

// Limits are how much one sweep takes and how long the relay waits on its stores. They are the
// deployment's numbers, set where the relay is wired (cmd/outbox-relay): the relay only holds
// itself to them.
type Limits struct {
	// Batch is the most rows one sweep claims and publishes.
	Batch int
	// SweepTimeout is the outer fence around one sweep's publish and commit, each of which
	// has a bound of its own: a database that stops answering cannot hold the claimed rows,
	// and the loop with them, past it.
	SweepTimeout time.Duration
	// BacklogTimeout is the leash on counting the backlog, which is a number for a dashboard,
	// not work: a count that hangs must not stop the gauge from moving for longer.
	BacklogTimeout time.Duration
}

func (l Limits) check() error {
	switch {
	case l.Batch <= 0:
		return ErrBatch
	case l.SweepTimeout <= 0 || l.BacklogTimeout <= 0:
		return ErrTimeout
	}
	return nil
}

type claimStore interface {
	Claim(ctx context.Context, limit int) ([]Record, error)
	MarkPublished(ctx context.Context, ids []int64, at time.Time) error
	Backlog(ctx context.Context) (int, error)
}

// observer is told what each sweep did, in the relay's own words. What those numbers are
// called once they leave here is the metrics adapter's business, not this package's. The
// backlog is reported separately because it can be unavailable on its own: a sweep that
// failed must still be counted as a failed sweep, and a backlog nobody could read must not
// be reported as a backlog of zero. The two are reported from different goroutines, so an
// observer must be safe for concurrent use.
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
	limits    Limits
	logger    *slog.Logger
	observer  observer
}

func NewRelay(outbox claimStore, publisher eventPublisher, tx txManager, now Clock, limits Limits, logger *slog.Logger, watch observer) (*Relay, error) {
	if err := limits.check(); err != nil {
		return nil, err
	}
	return &Relay{
		outbox:    outbox,
		publisher: publisher,
		tx:        tx,
		now:       now,
		limits:    limits,
		logger:    logger,
		observer:  watch,
	}, nil
}

// Sweep publishes one batch and reports how many records went out. The order is the whole
// point: the records are marked published only after the broker has acknowledged them, so
// a crash in between republishes an event rather than losing it. A consumer sees the
// duplicate; nobody sees a payment that was never announced.
func (r *Relay) Sweep(ctx context.Context) (int, error) {
	ctx, end := telemetry.Start(ctx, "outbox.Sweep")
	published, err := r.sweep(ctx)
	end(err)
	return published, err
}

func (r *Relay) sweep(ctx context.Context) (int, error) {
	published := 0

	err := r.tx.WithinTx(ctx, func(ctx context.Context) error {
		claimed, err := r.outbox.Claim(ctx, r.limits.Batch)
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
//
// The backlog is counted on a clock of its own. Counted at the end of a sweep, it froze
// through the very outage it exists for: a sweep publishing to a cluster that cannot
// answer does not end until its timeout, and exp-13 watched the gauge sit at 1 while 308
// records waited.
func (r *Relay) Run(ctx context.Context, every time.Duration) error {
	if every <= 0 {
		return fmt.Errorf("outbox: run every %s: %w", every, ErrInterval)
	}

	var backlogLoop sync.WaitGroup
	defer backlogLoop.Wait()
	backlogLoop.Go(func() {
		r.countBacklog(ctx, every)
	})

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		if r.sweepOnce(ctx) && ctx.Err() == nil {
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// sweepOnce reports whether the sweep came back full. A full batch means more records are
// likely waiting, and Run sweeps again at once instead of waiting for the tick: with a batch
// of 100 and a tick of 1 s, waiting would cap the relay at 100 events a second whatever the
// broker could take. A failed or partial sweep waits for the tick, so a broker that refuses
// everything is not hammered in a loop.
func (r *Relay) sweepOnce(ctx context.Context) bool {
	sweeping, cancel := context.WithTimeout(ctx, r.limits.SweepTimeout)
	defer cancel()

	started := r.now()
	published, err := r.Sweep(sweeping)
	if err != nil && ctx.Err() != nil {
		// Stopping, not failing: a sweep the shutdown cut short is not reported as a failed
		// one, or every rollout would show on the sweep-outcome panel.
		return false
	}
	r.observer.Swept(published, r.now().Sub(started), err)

	switch {
	case err != nil:
		r.logger.ErrorContext(ctx, "outbox sweep failed", "error", err)
		return false
	case published > 0:
		r.logger.InfoContext(ctx, "outbox published", "records", published)
	}
	return published == r.limits.Batch
}

func (r *Relay) countBacklog(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		r.reportBacklog(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// reportBacklog leaves the last known backlog standing when it cannot be read, rather than
// reporting a zero, which an operator would read as "nothing is waiting" at the worst
// possible moment.
func (r *Relay) reportBacklog(ctx context.Context) {
	bounded, cancel := context.WithTimeout(ctx, r.limits.BacklogTimeout)
	defer cancel()

	backlog, err := r.outbox.Backlog(bounded)
	if err != nil {
		if ctx.Err() == nil {
			r.logger.WarnContext(ctx, "outbox backlog unavailable", "error", err)
		}
		return
	}
	r.observer.Backlog(backlog)
}
