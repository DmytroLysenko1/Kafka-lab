package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	paymentv1 "github.com/DmytroLysenko1/Kafka-lab/gen/payment/v1"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
)

var (
	ErrConsume    = errors.New("kafka: consume")
	ErrDecode     = errors.New("kafka: decode record")
	ErrDeadLetter = errors.New("kafka: dead letter")
	ErrRetry      = errors.New("kafka: move to a retry tier")
)

const (
	maxRecordsPerPoll = 100
	handlingTimeout   = 15 * time.Second
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

type detours interface {
	Retry(ctx context.Context, record *kgo.Record, topic string, attempt int) error
	DeadLetter(ctx context.Context, record *kgo.Record, reason error, class Class) error
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

// Consumer is one stage's poll loop. paused belongs to that loop alone — Run is its only
// reader and writer — and says, per partition, when the record it was stopped at is due.
type Consumer struct {
	client   *kgo.Client
	stage    Stage
	handle   handler
	detours  detours
	logger   *slog.Logger
	observer observer
	paused   map[int32]time.Time
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
		paused:   make(map[int32]time.Time),
	}, nil
}

func (c *Consumer) Close() { c.client.Close() }

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

// untilNextDue bounds a poll by the earliest paused partition's due time. With nothing
// paused it is the caller's context unchanged.
func (c *Consumer) untilNextDue(ctx context.Context) (context.Context, context.CancelFunc) {
	if len(c.paused) == 0 {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, slices.MinFunc(slices.Collect(maps.Values(c.paused)), time.Time.Compare))
}

// pausedPartitionCameDue tells a poll that ended because a paused partition is due from
// one that ended because the consumer is stopping or the fetch failed. It judges by the
// error the poll returned, not by the poll's context read afterwards: that context may
// expire a moment after the poll came back with records and a real error, and those
// records would be dropped as if nothing had arrived.
func pausedPartitionCameDue(ctx context.Context, fetches kgo.Fetches, err error) bool {
	return ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) && fetches.NumRecords() == 0
}

// resumeDue starts fetching again every partition whose record is now due.
func (c *Consumer) resumeDue(now time.Time) {
	due := make([]int32, 0, len(c.paused))
	for partition, at := range c.paused {
		if !at.After(now) {
			due = append(due, partition)
		}
	}
	if len(due) == 0 {
		return
	}
	for _, partition := range due {
		delete(c.paused, partition)
	}
	c.client.ResumeFetchPartitions(map[string][]int32{c.stage.Topic: due})
}

func (c *Consumer) handleBatch(ctx context.Context, fetches kgo.Fetches) batch {
	outcome := batch{handled: make([]*kgo.Record, 0, maxRecordsPerPoll), waiting: make(map[int32]*kgo.Record)}
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
	if len(handled) == 0 {
		return nil
	}
	committing, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()

	if err := c.client.CommitRecords(committing, handled...); err != nil {
		c.observer.Failed(stageCommit)
		return fmt.Errorf("%w: commit %d records: %w", ErrConsume, len(handled), err)
	}
	return nil
}

// holdUntilDue moves each waiting partition back to its first unhandled record, and stops
// fetching the ones whose record is not due yet — those alone: a partition that has waited
// long enough, or one full of records that are due, must not sit behind another's delay.
// A partition stopped by the batch budget is only rewound; it has nothing to wait for.
// Both happen before the rebalance is released: franz-go documents SetOffsets as safe only
// while no revoke can run, and BlockRebalanceOnPoll is what holds revokes off until
// AllowRebalance. The waiting itself happens outside any batch, holding nothing — the shape
// exp-16 measured against waiting inside the batch.
func (c *Consumer) holdUntilDue(waiting map[int32]*kgo.Record) {
	if len(waiting) == 0 {
		return
	}

	now := time.Now()
	rewind := make(map[int32]kgo.EpochOffset, len(waiting))
	notDue := make([]int32, 0, len(waiting))
	for partition, record := range waiting {
		rewind[partition] = kgo.EpochOffset{Epoch: -1, Offset: record.Offset}
		if wait := c.stage.untilDue(record, now); wait > 0 {
			c.paused[partition] = now.Add(wait)
			notDue = append(notDue, partition)
		}
	}

	c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{c.stage.Topic: rewind})
	if len(notDue) > 0 {
		c.client.PauseFetchPartitions(map[string][]int32{c.stage.Topic: notDue})
	}
}

// handleRecord returns an error only when the record must stay where it is: every other
// outcome — counted, a duplicate, moved to a retry tier, archived — lets the offset move.
func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record) error {
	event, err := decodeEvent(record)
	if err != nil {
		return c.deadLetter(ctx, record, err, ClassUndecodable)
	}

	handling, cancel := context.WithTimeout(ctx, handlingTimeout)
	defer cancel()

	started := time.Now()
	counted, err := c.handle.Execute(handling, event)
	switch {
	case errors.Is(err, merchants.ErrUnprocessable):
		return c.deadLetter(ctx, record, err, ClassRefused)
	case errors.Is(err, merchants.ErrContended):
		return c.moveOn(ctx, record, err)
	case err != nil:
		c.observer.Failed(stageHandle)
		return err
	}
	c.observer.Handled(counted, time.Since(started))

	c.logger.DebugContext(ctx, "record handled",
		"event_id", event.EventID,
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"counted", counted,
	)
	return nil
}

// moveOn takes a contended record off the partition so the payments behind it are not
// held for a lock none of them needs. Past the last tier it has had every chance the chain
// gives, and it is archived for a person to look at.
func (c *Consumer) moveOn(ctx context.Context, record *kgo.Record, reason error) error {
	if c.stage.Next == "" {
		return c.deadLetter(ctx, record, reason, ClassExhausted)
	}
	if err := c.detours.Retry(ctx, record, c.stage.Next, c.stage.Tier+1); err != nil {
		c.observer.Failed(stageRetry)
		return fmt.Errorf("%w: %w", ErrRetry, err)
	}
	c.logger.WarnContext(ctx, "record moved to a retry tier",
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"next", c.stage.Next,
		"reason", reason.Error(),
	)
	return nil
}

// deadLetter is the last way a record leaves the partition unhandled. If the dead letter
// topic itself refuses it, the offset stays put: a record nobody can store is still better
// stuck than silently gone.
func (c *Consumer) deadLetter(ctx context.Context, record *kgo.Record, reason error, class Class) error {
	if err := c.detours.DeadLetter(ctx, record, reason, class); err != nil {
		c.observer.Failed(stageDeadLetter)
		return fmt.Errorf("%w: %w", ErrDeadLetter, err)
	}
	c.logger.ErrorContext(ctx, "record sent to the dead letter topic",
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"class", class,
		"reason", reason.Error(),
	)
	return nil
}

// decodeEvent reads the record the way the wire format says to: the schema id and the
// message indexes come off the front, and what remains is the protobuf. The event id comes
// from the header the relay set — the outbox row id, which survives a republished record
// and is therefore what deduplication has to key on.
func decodeEvent(record *kgo.Record) (merchants.AuthorizedEvent, error) {
	header := sr.ConfluentHeader{}

	id, body, err := header.DecodeID(record.Value)
	if err != nil {
		return merchants.AuthorizedEvent{}, fmt.Errorf("%w: schema id: %w", ErrDecode, err)
	}
	_, body, err = header.DecodeIndex(body, 1)
	if err != nil {
		return merchants.AuthorizedEvent{}, fmt.Errorf("%w: schema %d message index: %w", ErrDecode, id, err)
	}

	var authorized paymentv1.PaymentAuthorized
	if err := proto.Unmarshal(body, &authorized); err != nil {
		return merchants.AuthorizedEvent{}, fmt.Errorf("%w: schema %d: %w", ErrDecode, id, err)
	}

	eventID := headerValue(record.Headers, "event_id")
	return merchants.AuthorizedEvent{
		EventID:     eventID,
		MerchantID:  authorized.GetMerchantId(),
		AmountMinor: authorized.GetAmountMinor(),
	}, nil
}
