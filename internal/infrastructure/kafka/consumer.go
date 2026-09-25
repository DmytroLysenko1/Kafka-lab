package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
)

var ErrConsume = errors.New("kafka: consume")

const (
	maxRecordsPerPoll = 100
	// The commit must survive the signal that stopped the consumer — dropping it would
	// replay work already done — but it must not be able to wait forever on a broker that
	// has stopped answering, or shutdown never finishes and the orchestrator kills it.
	commitTimeout = 5 * time.Second
	// batchBudget bounds how long one batch holds the rebalance. A contended record spends
	// its lock wait inside the batch, and a partition full of one locked merchant's payments
	// would otherwise hold the group past its rebalance timeout (60 s in franz-go) — the
	// member is removed, and its late commit can move an offset backwards (exp-16). Past
	// the budget the rest of the batch is rewound and fetched again by the next poll. The
	// budget plus the slowest single record (handling, then a detour) stays under 60 s.
	batchBudget = 20 * time.Second
	// tierFetchWait is how long a broker may hold a tier's fetch open when it has nothing
	// new. A partition resumed while a fetch is already in flight joins only the next one,
	// so with franz-go's default of 5 s a record due now was handled up to 5 s late — in
	// exp-18, 5–8 s after a replay into a 3 s tier. The main topic keeps the default: data
	// arriving ends its fetch at once, and it never pauses.
	tierFetchWait = 500 * time.Millisecond
)

type handler interface {
	Execute(ctx context.Context, event merchants.AuthorizedEvent) (bool, error)
}

// observer hears what this stage did with a record it kept: counted it, or failed on it.
// Where a record went instead is reported by the detours that sent it there. The stage is
// one of a handful of fixed words, never the error text: an error carries offsets and
// identifiers, and a label made of those grows without limit.
type observer interface {
	Handled(counted bool, took time.Duration)
	Failed(stage string)
}

// The stages a record's path can fail at. A fixed set, so the label cannot grow.
const (
	stageFetch      = "fetch"
	stageHandle     = "handle"
	stageCommit     = "commit"
	stageRetry      = "retry"
	stageDeadLetter = "dead_letter"
)

// Consumer is one stage's poll loop: this file is the loop and its commits, routing.go
// what happens to each record, pauses.go how a tier waits, decode.go the wire format.
type Consumer struct {
	client   *kgo.Client
	stage    Stage
	handle   handler
	detours  detours
	logger   *slog.Logger
	observer observer
	paused   pauseSchedule
}

// NewConsumer turns autocommit off: the offset must move only after the database has the
// result, and franz-go's autocommit runs on a timer that knows nothing about that.
func NewConsumer(brokers []string, stage Stage, handle handler, routes detours, logger *slog.Logger, watch observer) (*Consumer, error) {
	client, err := kgo.NewClient(append(stage.clientOptions(),
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(stage.Group),
		kgo.ConsumeTopics(stage.Topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// A rebalance in the middle of a polled batch would hand these partitions to another
		// member while this one is still writing their records. Blocking it until the batch
		// is committed is what keeps "handled" and "committed" the same set.
		kgo.BlockRebalanceOnPoll(),
	)...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConsume, err)
	}
	return &Consumer{
		client:   client,
		stage:    stage,
		handle:   handle,
		detours:  routes,
		logger:   logger,
		observer: watch,
		paused:   make(pauseSchedule),
	}, nil
}

func (c *Consumer) Close() {
	c.client.Close()
}

func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		c.resumeDue(time.Now())
		if err := c.poll(ctx); err != nil {
			// Only a cancellation is an orderly stop. A deadline is not: the ones that can
			// reach here come from this consumer's own timeouts — the offset commit and the
			// handling of a record — and reporting either as a clean exit would turn a
			// failed commit into a silent one.
			if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
				break
			}
			return err
		}
	}
	return nil
}

// batch is one poll's worth of outcomes: what was handled and may be committed, the first
// record of each partition that is not due yet, and the failure that stopped the batch.
type batch struct {
	handled []*kgo.Record
	waiting map[int32]*kgo.Record
	failure error
}

// poll handles one batch and commits exactly what it handled. A record it could not handle
// stops the batch with the offset where it was: the next attempt starts there, and nothing
// after it is marked done on the strength of a record that never was. While partitions are
// paused, the poll gives up at the moment the first of them is due, so it can be resumed;
// the partitions that are not paused are fetched as usual in the meantime.
func (c *Consumer) poll(ctx context.Context) error {
	polling, cancel := c.untilNextDue(ctx)
	defer cancel()

	fetches := c.client.PollRecords(polling, maxRecordsPerPoll)
	defer c.client.AllowRebalance()

	if err := fetches.Err(); err != nil {
		if pausedPartitionCameDue(ctx, fetches, err) {
			return nil
		}
		c.observer.Failed(stageFetch)
		return fmt.Errorf("%w: %w", ErrConsume, err)
	}

	outcome := c.handleBatch(ctx, fetches)
	if err := c.commit(ctx, outcome.handled); err != nil {
		return err
	}
	if outcome.failure != nil {
		return outcome.failure
	}
	c.holdUntilDue(outcome.waiting)
	return nil
}

func (c *Consumer) handleBatch(ctx context.Context, fetches kgo.Fetches) batch {
	outcome := batch{
		handled: make([]*kgo.Record, 0, maxRecordsPerPoll),
		waiting: make(map[int32]*kgo.Record),
	}
	now := time.Now()
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		if outcome.failure == nil {
			c.handlePartition(ctx, partition.Records, now, &outcome)
		}
	})
	return outcome
}

// handlePartition stops a partition at its first record that is not due: records within a
// partition of one stage are due in the order they arrived, so nothing after it is due
// either. Other partitions carry on — they may hold records that have waited long enough.
// A batch that has used up its budget stops the same way, with nothing to wait for.
func (c *Consumer) handlePartition(ctx context.Context, records []*kgo.Record, now time.Time, outcome *batch) {
	for _, record := range records {
		if c.stage.untilDue(record, now) > 0 || time.Since(now) > batchBudget {
			outcome.waiting[record.Partition] = record
			return
		}
		if err := c.handleRecord(ctx, record); err != nil {
			outcome.failure = err
			return
		}
		outcome.handled = append(outcome.handled, record)
	}
}

func (c *Consumer) commit(ctx context.Context, handled []*kgo.Record) error {
	if err := commitRecords(ctx, c.client, handled); err != nil {
		c.observer.Failed(stageCommit)
		return fmt.Errorf("%w: commit %d records: %w", ErrConsume, len(handled), err)
	}
	return nil
}

// commitRecords commits on a context the caller's cancellation cannot reach, bounded by
// commitTimeout: see the constant for why both halves matter.
func commitRecords(ctx context.Context, client *kgo.Client, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	committing, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()
	return client.CommitRecords(committing, records...)
}
