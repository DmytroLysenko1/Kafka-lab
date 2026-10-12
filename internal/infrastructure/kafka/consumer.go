package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
)

var ErrConsume = errors.New("kafka: consume")

const (
	// rebalanceTimeout is how long the group waits for a member to finish its batch and rejoin
	// before it is removed — and a removed member's late commit can move an offset backwards
	// (exp-16). Everything a batch may spend is budgeted under it (batchBudget).
	rebalanceTimeout = 60 * time.Second
	// maxRecordsPerPoll is the Java client's max.poll.records default (500) cut to a fifth: a
	// batch is committed as a whole, so a crash replays at most this many records, each
	// deduplicated by the inbox; and batchBudget, not the count, is what bounds the time.
	maxRecordsPerPoll = 100
	// The commit must survive the signal that stopped the consumer — dropping it would
	// replay work already done — but it must not be able to wait forever on a broker that
	// has stopped answering, or shutdown never finishes and the orchestrator kills it.
	// On a stop the in-flight record settles first: its rollback (5 s), a detour to a retry
	// tier (18 s), then this commit — 28 s at most, inside the pod's 30 s
	// terminationGracePeriodSeconds (deploy/helm/payments/values.yaml).
	commitTimeout = 5 * time.Second
	// batchBudget bounds how long one batch holds the rebalance. A contended record spends
	// its lock wait inside the batch, and a partition full of one locked merchant's payments
	// would otherwise hold the group past its rebalance timeout (60 s in franz-go) — the
	// member is removed, and its late commit can move an offset backwards (exp-16). Past
	// the budget the rest of the batch is rewound and fetched again by the next poll. The
	// budget plus the slowest single record plus the commit stays under 60 s: 15 + 15
	// (handlingTimeout) + 5 (the rollback of a handling that failed, postgres.RollbackTimeout)
	// + 18 (detourTimeout) + 5 (commitTimeout) = 58 s.
	batchBudget = 15 * time.Second
	// tierFetchWait is how long a broker may hold a tier's fetch open when it has nothing
	// new. A partition resumed while a fetch is already in flight joins only the next one,
	// so with franz-go's default of 5 s a record due now could be handled up to 5 s late; at
	// 500 ms exp-18's logged runs handle a record replayed into a 3 s tier 3.0–3.5 s after it
	// entered. The main topic keeps the default: data arriving ends its fetch at once, and it
	// never pauses.
	tierFetchWait = 500 * time.Millisecond
)

type handler interface {
	Execute(ctx context.Context, event *merchants.PaymentEvent) (bool, error)
}

// observer hears what this stage did with a record it kept: counted it, or failed on it.
// Where a record went instead is reported by the detours that sent it there. The stage is
// one of a handful of fixed words, never the error text: an error carries offsets and
// identifiers, and a label made of those grows without limit.
type observer interface {
	Handled(outcome string)
	HandlingTook(outcome string, took time.Duration)
	Failed(stage string)
}

// What one attempt at a record came to. A fixed set, so the label cannot grow.
const (
	outcomeCounted   = "counted"
	outcomeDuplicate = "duplicate"
	outcomeContended = "contended"
	outcomeRefused   = "refused"
	outcomeFailed    = "failed"
)

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
	tracer   *kotel.Tracer
}

// NewConsumer joins stage's group with the options groupOptions explains.
func NewConsumer(brokers []string, stage Stage, handle handler, routes detours, logger *slog.Logger, watch observer) (*Consumer, error) {
	tracer, traced := tracing()
	client, err := kgo.NewClient(append(groupOptions(brokers, stage), traced)...)
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
		tracer:   tracer,
	}, nil
}

// groupOptions turn autocommit off: the offset must move only after the database has the
// result, and franz-go's autocommit runs on a timer that knows nothing about that.
func groupOptions(brokers []string, stage Stage) []kgo.Opt {
	return append(stage.clientOptions(),
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(stage.Group),
		kgo.ConsumeTopics(stage.Topic),
		kgo.DisableAutoCommit(),
		// A group with no committed offset — new, or expired — starts from the oldest
		// record still in the log, not from now: a new projection must count the whole
		// window the topic keeps, and every record it counts twice is dropped by the inbox.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// A rebalance in the middle of a polled batch would hand these partitions to another
		// member while this one is still writing their records. Blocking it until the batch
		// is committed is what keeps "handled" and "committed" the same set.
		kgo.BlockRebalanceOnPoll(),
		// Set, not left to franz-go's default of the same value: the batch budget is computed
		// against it (budget_test.go), and a default that moved would break that silently.
		kgo.RebalanceTimeout(rebalanceTimeout),
	)
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
			if stopping(err) {
				break
			}
			return err
		}
	}
	return nil
}

// sortFetchErrors separates the fetch errors franz-go documents as informational — the client
// recovers from them by itself (kgo.Fetches.Errors) — from those that stop the consumer. A lost
// group session (a broker unreachable for longer than the session timeout, a rebalance held too
// long) and data loss (a log truncated under the consumer, which the client resets past) are
// counted and logged, and the consumer carries on: exiting for them would add the kubelet's
// restart back-off to an outage the client was already riding out. Anything else is returned.
func (c *Consumer) sortFetchErrors(ctx context.Context, fetches kgo.Fetches) (bool, error) {
	var fatal error
	sessionLost := false
	for _, failed := range fetches.Errors() {
		_, lost := errors.AsType[*kgo.ErrGroupSession](failed.Err)
		_, truncated := errors.AsType[*kgo.ErrDataLoss](failed.Err)
		switch {
		case lost || truncated:
			sessionLost = sessionLost || lost
			c.observer.Failed(stageFetch)
			c.logger.WarnContext(ctx, "fetch reported a condition the client recovers from",
				"topic", failed.Topic, "partition", failed.Partition, "error", failed.Err)
		case fatal == nil:
			fatal = failed.Err
		}
	}
	return sessionLost, fatal
}

// stopping is whether err is the consumer being stopped rather than something failing: an
// error caused by the cancellation of its context, or by the client closing. It is judged by
// the error, never by the context's state afterwards: a context cancelled a moment after a real
// failure — a SIGTERM during a rollout over a flapping database, or a sibling stage's failure
// cancelling the group — must not hide that failure, and a failure must not turn a stop into
// a non-zero exit. The cause of the cancellation is not consulted either: a stage cancelled
// because a sibling failed is being stopped, and the sibling reports the failure itself.
func stopping(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed)
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

	sessionLost, fatal := c.sortFetchErrors(ctx, fetches)
	switch {
	case fatal != nil:
		return c.fetchFailed(ctx, fetches, fatal)
	case sessionLost:
		// The records in this poll belong to a session the group no longer has: committing
		// their offsets would be refused, so they are left to be fetched again once the client
		// has rejoined — and the inbox drops any of them that were already counted.
		return nil
	}
	return c.settle(ctx, c.handleBatch(ctx, fetches))
}

// fetchFailed reports a poll that could not fetch. A paused partition coming due ends the poll
// on purpose, and a stop is not a failure.
func (c *Consumer) fetchFailed(ctx context.Context, fetches kgo.Fetches, fatal error) error {
	if pausedPartitionCameDue(ctx, fetches, fatal) {
		return nil
	}
	if !stopping(fatal) {
		c.observer.Failed(stageFetch)
	}
	return fmt.Errorf("%w: %w", ErrConsume, fatal)
}

// settle commits what the batch handled before anything else: a failure that stopped the batch
// is reported only after the work done ahead of it is safe.
func (c *Consumer) settle(ctx context.Context, outcome batch) error {
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
