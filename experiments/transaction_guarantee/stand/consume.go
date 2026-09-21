package stand

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

// The autocommit modes are timed so that the kill can land right after an autocommit, which
// is the only moment the two flavours differ. 100 ms is the shortest interval franz-go
// accepts, and 5 ms a record makes a batch of 50 last about 250 ms, so an autocommit lands
// inside every batch. Neither value is advice.
const (
	autocommitInterval    = 100 * time.Millisecond
	autocommitHandleDelay = 5 * time.Millisecond
)

// consume reads the run and writes it to Postgres. Which semantics it demonstrates is
// decided entirely by when the offset is committed relative to the write.
func consume(ctx context.Context, cfg *Settings, db *store) error {
	group := fmt.Sprintf("%s-%s", cfg.Name, cfg.RunID)
	client, err := kgo.NewClient(clientOptions(cfg, group)...)
	if err != nil {
		return fmt.Errorf("%s: kafka client: %w", cfg.Name, err)
	}
	defer client.Close()

	drained, err := labkit.GroupProgress(ctx, kadm.NewClient(client), cfg.Topic, group)
	if err != nil {
		return err
	}

	run := &consumer{cfg: cfg, db: db, client: client}
	for !drained.Done() {
		fetches := client.PollRecords(ctx, PollBatch)
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("%s: read %s with %d left: %w", cfg.Name, cfg.Topic, drained.Remaining(), err)
		}

		batch, err := decode(fetches, cfg)
		if err != nil {
			return err
		}
		run.polled(fetches)
		if err := run.apply(ctx, fetches, batch); err != nil {
			return err
		}
		fetches.EachRecord(func(record *kgo.Record) { drained.Saw(record.Partition, record.Offset) })
	}

	// Reaching the end with nothing left to commit still has to commit: the last batch of
	// an at-least-once run is only safe once its offsets are stored.
	if err := client.CommitUncommittedOffsets(ctx); err != nil {
		return fmt.Errorf("%s: commit final offsets: %w", cfg.Name, err)
	}
	return nil
}

func clientOptions(cfg *Settings, group string) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(cfg.Brokers, ",")...),
		kgo.ConsumeTopics(cfg.Topic),
		// One group per run, so a restarted process resumes where the killed one committed
		// — which is the whole mechanism under test — while a later run starts from
		// nothing.
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	if !cfg.Mode.AutoCommits() {
		return append(opts, kgo.DisableAutoCommit())
	}
	opts = append(opts, kgo.AutoCommitInterval(autocommitInterval))
	if cfg.Mode == AutoCommitGreedy {
		opts = append(opts, kgo.GreedyAutoCommit())
	}
	return opts
}

type consumer struct {
	cfg     *Settings
	db      *store
	client  *kgo.Client
	handled int
	// For the autocommit modes: what this client had committed when the current batch was
	// polled, and the offset just past the batch on each of its partitions.
	committedAtPoll map[int32]int64
	batchEnd        map[int32]int64
}

func (c *consumer) polled(fetches kgo.Fetches) {
	if !c.cfg.Mode.AutoCommits() {
		return
	}
	c.committedAtPoll = c.committed()
	c.batchEnd = make(map[int32]int64)
	fetches.EachRecord(func(record *kgo.Record) {
		c.batchEnd[record.Partition] = max(c.batchEnd[record.Partition], record.Offset+1)
	})
}

// committed is what the broker has accepted from this client's commits, per partition.
func (c *consumer) committed() map[int32]int64 {
	offsets := make(map[int32]int64)
	for partition, at := range c.client.CommittedOffsets()[c.cfg.Topic] {
		offsets[partition] = at.Offset
	}
	return offsets
}

// autocommitLanded decides when the autocommit modes may die. Greedy waits until an
// autocommit covers the whole batch being written, which is the commit that makes its unwritten
// rest unreachable. The default waits until any autocommit has landed since the poll: it
// commits what the previous poll returned, so the batch being written stays uncommitted
// either way, and the run shows that.
func autocommitLanded(mode Mode, atPoll, now, batchEnd map[int32]int64) bool {
	if mode != AutoCommitGreedy {
		return !maps.Equal(atPoll, now)
	}
	for partition, end := range batchEnd {
		if now[partition] < end {
			return false
		}
	}
	return len(batchEnd) > 0
}

func (c *consumer) apply(ctx context.Context, fetches kgo.Fetches, batch []Payment) error {
	if c.cfg.Mode.AutoCommits() {
		return c.writeAll(ctx, batch)
	}
	if c.cfg.Mode.CommitsFirst() {
		if err := c.commit(ctx, fetches); err != nil {
			return err
		}
		return c.writeAll(ctx, batch)
	}

	if err := c.writeAll(ctx, batch); err != nil {
		return err
	}
	return c.commit(ctx, fetches)
}

func (c *consumer) writeAll(ctx context.Context, batch []Payment) error {
	for i, handled := range batch {
		if err := c.write(ctx, handled); err != nil {
			return err
		}
		if c.cfg.Mode.AutoCommits() {
			time.Sleep(autocommitHandleDelay)
		}
		c.handled++
		if c.shouldDie(i == len(batch)-1) {
			labkit.Crash(c.cfg.Name+": killing this consumer", "mode", c.cfg.Mode, "handled", c.handled)
		}
	}
	return nil
}

// shouldDie decides where the crash lands, and under at-most-once that is not a detail.
// The offsets of the batch are already committed, so the kill has to leave records of that
// batch unwritten to lose anything at all: landing on the last record of a batch commits
// exactly what was written and the run proves nothing. The first live run did precisely
// that — 500 handled fell on a batch boundary, and the report read "every payment exactly
// once" for the semantics that is supposed to lose them.
//
// Under autocommit the kill waits for an autocommit to land inside the batch and for records
// of the batch to remain unwritten. Then the two flavours differ in exactly one thing: greedy
// has just committed this batch, so its unwritten rest is lost; the default has committed
// only the previous batch, so the written part of this one is replayed.
func (c *consumer) shouldDie(lastOfBatch bool) bool {
	if c.cfg.DieAfter <= 0 || c.handled < c.cfg.DieAfter {
		return false
	}
	switch {
	case c.cfg.Mode.AutoCommits():
		return !lastOfBatch && autocommitLanded(c.cfg.Mode, c.committedAtPoll, c.committed(), c.batchEnd)
	case c.cfg.Mode.CommitsFirst():
		return !lastOfBatch
	default:
		return true
	}
}

func (c *consumer) write(ctx context.Context, handled Payment) error {
	if c.cfg.Mode != Inbox {
		return c.db.record(ctx, c.cfg.RunID, c.cfg.Mode, handled)
	}
	return c.db.recordOnce(ctx, c.cfg.RunID, c.cfg.Mode, handled)
}

func (c *consumer) commit(ctx context.Context, fetches kgo.Fetches) error {
	if err := c.client.CommitRecords(ctx, records(fetches)...); err != nil {
		return fmt.Errorf("%s: commit offsets: %w", c.cfg.Name, err)
	}
	return nil
}

func decode(fetches kgo.Fetches, cfg *Settings) ([]Payment, error) {
	batch := make([]Payment, 0, PollBatch)

	var failure error
	fetches.EachRecord(func(record *kgo.Record) {
		if failure != nil {
			return
		}
		var decoded Payment
		if err := json.Unmarshal(record.Value, &decoded); err != nil {
			failure = fmt.Errorf("%s: undecodable record at %s offset %d: %w", cfg.Name, cfg.Topic, record.Offset, err)
			return
		}
		if decoded.RunID != cfg.RunID {
			return
		}
		batch = append(batch, decoded)
	})
	return batch, failure
}

func records(fetches kgo.Fetches) []*kgo.Record {
	all := make([]*kgo.Record, 0, PollBatch)
	fetches.EachRecord(func(record *kgo.Record) { all = append(all, record) })
	return all
}
