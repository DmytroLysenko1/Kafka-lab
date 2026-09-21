package stand

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// consume reads the run and writes it to Postgres. Which of the three semantics it
// demonstrates is decided entirely by where CommitRecords sits relative to the write.
func consume(ctx context.Context, cfg *Settings, db *store) error {
	group := fmt.Sprintf("%s-%s", cfg.Name, cfg.RunID)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.Brokers, ",")...),
		kgo.ConsumeTopics(cfg.Topic),
		// One group per run, so a restarted process resumes where the killed one committed
		// — which is the whole mechanism under test — while a later run starts from
		// nothing.
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("%s: kafka client: %w", cfg.Name, err)
	}
	defer client.Close()

	drained, err := endOffsets(ctx, cfg, group)
	if err != nil {
		return err
	}

	run := &consumer{cfg: cfg, db: db, client: client}
	for !drained.reached() {
		fetches := client.PollRecords(ctx, PollBatch)
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("%s: read %s with %d left: %w", cfg.Name, cfg.Topic, drained.remaining(), err)
		}

		batch, err := decode(fetches, cfg)
		if err != nil {
			return err
		}
		if err := run.apply(ctx, fetches, batch); err != nil {
			return err
		}
		fetches.EachRecord(func(record *kgo.Record) { drained.saw(record.Partition, record.Offset) })
	}

	// Reaching the end with nothing left to commit still has to commit: the last batch of
	// an at-least-once run is only safe once its offsets are stored.
	if err := client.CommitUncommittedOffsets(ctx); err != nil {
		return fmt.Errorf("%s: commit final offsets: %w", cfg.Name, err)
	}
	return nil
}

type consumer struct {
	cfg     *Settings
	db      *store
	client  *kgo.Client
	handled int
}

func (c *consumer) apply(ctx context.Context, fetches kgo.Fetches, batch []Payment) error {
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
		c.handled++
		if c.shouldDie(i == len(batch)-1) {
			die(c.cfg, c.handled)
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
func (c *consumer) shouldDie(lastOfBatch bool) bool {
	if c.cfg.DieAfter <= 0 || c.handled < c.cfg.DieAfter {
		return false
	}
	return !c.cfg.Mode.CommitsFirst() || !lastOfBatch
}

func (c *consumer) write(ctx context.Context, handled Payment) error {
	if c.cfg.Mode != Inbox {
		return c.db.record(ctx, c.cfg.RunID, c.cfg.Mode, handled)
	}
	_, err := c.db.recordOnce(ctx, c.cfg.RunID, c.cfg.Mode, handled)
	return err
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
