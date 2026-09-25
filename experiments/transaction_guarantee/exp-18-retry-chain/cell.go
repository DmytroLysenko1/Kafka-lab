package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
)

var errDatabase = errors.New("exp-18: the database refused the write")

// progressPoll is how often the cell checks whether every payment has been counted, and
// how often cell C's operator looks at the dead letter topic.
const progressPoll = 200 * time.Millisecond

type tier struct {
	suffix string
	delay  time.Duration
}

// chainTiers are the catalog's 5s/1m/10m scaled down so a cell takes a minute or two.
func chainTiers() []tier {
	return []tier{
		{suffix: "retry.3s", delay: 3 * time.Second},
		{suffix: "retry.6s", delay: 6 * time.Second},
		{suffix: "retry.12s", delay: 12 * time.Second},
	}
}

// timeline is what happened when, measured from the moment the consumers started. Each
// field has one writer; the report reads them only after every writer has been joined.
type timeline struct {
	started  time.Time
	released time.Time
	replayed time.Time
	replay   kafka.ReplayReport
	restarts int
	drained  bool
}

type cell struct {
	s       *settings
	admin   *kadm.Client
	pool    *pgxpool.Pool
	record  *merchants.RecordAuthorized
	detours *kafka.Detours
	lock    lock
	counted func() bool
	line    timeline
}

func measureCell(ctx context.Context, s *settings) (err error) {
	pool, err := labkit.Postgres(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer pool.Close()
	storage, err := postgres.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer storage.Close()

	produced, err := produce(ctx, s)
	if err != nil {
		return err
	}
	held, err := lockMerchant(ctx, pool, s.hotMerchant())
	if err != nil {
		return err
	}
	// The operator's release is the one the timings rest on; this one covers an early
	// return, and its failure still reaches the caller.
	defer func() { err = errors.Join(err, held.release()) }()
	detours, err := kafka.NewDetours(brokers(), s.topic("dlq"), metrics.Discard{})
	if err != nil {
		return err
	}
	defer detours.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	c := cell{
		s:       s,
		admin:   admin,
		pool:    pool,
		record:  merchants.NewRecordAuthorized(postgres.NewInboxStore(storage), postgres.NewTotalsStore(storage), storage, time.Now),
		detours: detours,
		lock:    held,
		counted: func() bool {
			n, err := countInbox(ctx, pool, produced.all)
			return err == nil && n == len(produced.all)
		},
	}
	if err := c.run(ctx); err != nil {
		return err
	}
	return report(ctx, os.Stdout, pool, admin, s, produced, &c.line)
}

func (c *cell) run(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, c.s.budget)
	defer cancel()
	finished, finish := context.WithCancel(bounded)
	defer finish()

	c.line.started = time.Now()
	work, workCtx := errgroup.WithContext(finished)
	work.Go(func() error { return c.operate(workCtx) })
	if c.s.chained() {
		c.runChain(workCtx, work, finish)
	} else {
		work.Go(func() error {
			defer finish()
			restarts, drained, err := labkit.Supervise(workCtx, c.startBlind, c.counted)
			c.line.restarts, c.line.drained = restarts, drained
			return err
		})
	}
	return work.Wait()
}

// runChain starts every stage of the chain and a watcher that ends the cell once every
// payment is counted. A stage that dies ends the cell with its error: in these cells the
// only failure the chain is meant to absorb is contention, and anything else is a finding.
func (c *cell) runChain(ctx context.Context, work *errgroup.Group, finish context.CancelFunc) {
	for _, stage := range c.stages() {
		consumer, err := kafka.NewConsumer(brokers(), stage, c.record, c.detours, slog.New(slog.DiscardHandler), metrics.Discard{})
		if err != nil {
			work.Go(func() error {
				return err
			})
			return
		}
		work.Go(func() error {
			defer consumer.Close()
			return consumer.Run(ctx)
		})
	}
	work.Go(func() error {
		c.line.drained = waitUntil(ctx, c.counted)
		finish()
		return nil
	})
}

func (c *cell) stages() []kafka.Stage {
	chainOf := chainTiers()
	chain := make([]kafka.Stage, 0, 1+len(chainOf))
	chain = append(chain, kafka.Stage{
		Topic: c.s.topic("main"),
		Group: c.s.group("main"),
	})
	for _, next := range chainOf {
		chain = append(chain, kafka.Stage{
			Topic: c.s.topic(next.suffix),
			Group: c.s.group(next.suffix),
			Delay: next.delay,
		})
	}
	return kafka.Chain(chain...)
}

// startBlind is cell A's consumer: the same code, told nothing about contention. Every
// database failure looks alike to it, so it does what it does for all of them — holds the
// offset and dies — which is what the service did before the chain existed.
func (c *cell) startBlind() (labkit.Runner, error) {
	return kafka.NewConsumer(brokers(), kafka.Stage{Topic: c.s.topic("main"), Group: c.s.group("main")},
		blindToContention{
			inner: c.record,
		}, c.detours, slog.New(slog.DiscardHandler), metrics.Discard{})
}

// operate is the person or the job on the other side of the lock. In cells A and B it lets
// go after the hold; in cell C it waits until every contended payment has run through the
// chain and been archived, lets go, and replays them.
func (c *cell) operate(ctx context.Context) error {
	if c.s.cell != cellExhausted {
		if !sleep(ctx, c.s.hold) {
			return nil
		}
		return c.release()
	}

	archived := func() bool {
		n, err := endOffsets(ctx, c.admin, c.s.topic("dlq"))
		return err == nil && n >= int64(c.s.hotCount())
	}
	if !waitUntil(ctx, archived) {
		return nil
	}
	if err := c.release(); err != nil {
		return err
	}
	report, err := kafka.Replay(ctx, brokers(), kafka.ReplayRequest{
		DeadLetterTopic: c.s.topic("dlq"),
		Class:           kafka.ClassExhausted,
		Into:            c.s.topic(chainTiers()[0].suffix),
		Group:           c.s.group("replayer"),
	}, c.detours)
	c.line.replay, c.line.replayed = report, time.Now()
	return unlessOutOfBudget(ctx, err)
}

// unlessOutOfBudget drops a failure caused by the budget running out: the cell is then
// reported as never drained, which is what happened, rather than as a crash of the
// instrument. Any other failure stands.
func unlessOutOfBudget(ctx context.Context, err error) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		return err
	}
}

func (c *cell) release() error {
	if err := c.lock.release(); err != nil {
		return fmt.Errorf("release the merchant's row: %w", err)
	}
	c.line.released = time.Now()
	return nil
}

// blindToContention hides the one thing the chain is built on: its error still reports a
// database failure, but no longer says which kind.
type blindToContention struct {
	inner *merchants.RecordAuthorized
}

func (b blindToContention) Execute(ctx context.Context, event merchants.AuthorizedEvent) (bool, error) {
	counted, err := b.inner.Execute(ctx, event)
	if errors.Is(err, merchants.ErrContended) {
		return counted, fmt.Errorf("%w: %s", errDatabase, err.Error())
	}
	return counted, err
}

func endOffsets(ctx context.Context, admin *kadm.Client, topic string) (int64, error) {
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return 0, fmt.Errorf("end offsets of %s: %w", topic, err)
	}
	var total int64
	ends.Each(func(end kadm.ListedOffset) {
		total += end.Offset
	})
	return total, nil
}

func waitUntil(ctx context.Context, reached func() bool) bool {
	ticker := time.NewTicker(progressPoll)
	defer ticker.Stop()
	for !reached() {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
	return true
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
