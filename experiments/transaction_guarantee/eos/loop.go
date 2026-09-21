package eos

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

// payment is both the input and the output event: the loop's work is to move it.
type payment struct {
	RunID     string `json:"run_id"`
	PaymentID string `json:"payment_id"`
}

func seed(ctx context.Context, cfg *Settings) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.brokers()...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return fmt.Errorf("%s: kafka client: %w", cfg.Name, err)
	}
	defer client.Close()

	records := make([]*kgo.Record, 0, cfg.Payments)
	for i := range cfg.Payments {
		id := fmt.Sprintf("%s-%05d", cfg.RunID, i)
		value, err := json.Marshal(payment{RunID: cfg.RunID, PaymentID: id})
		if err != nil {
			return fmt.Errorf("%s: encode %s: %w", cfg.Name, id, err)
		}
		records = append(records, &kgo.Record{Topic: cfg.Input, Key: []byte(id), Value: value})
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("%w: %w", ErrPartial, err)
	}
	return nil
}

// process is the read-process-write loop. Each batch is one transaction: the output
// records and the consumed offsets commit together or not at all. The Postgres write, when
// there is one, happens inside the loop and outside the transaction — which is the whole of
// what exp-10b is about.
func process(ctx context.Context, cfg *Settings) error {
	session, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(cfg.brokers()...),
		kgo.TransactionalID(cfg.transactionalID()),
		kgo.ConsumerGroup(cfg.group()),
		kgo.ConsumeTopics(cfg.Input),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// read_committed on the input as well: a loop reading another transactional
		// producer's aborted records would process work that never happened. Stable offset
		// fetch (KIP-447) needs no option — franz-go now enables it for every group.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return fmt.Errorf("%s: transactional session: %w", cfg.Name, err)
	}
	defer session.Close()

	var db *pgxpool.Pool
	if cfg.WriteDB {
		if db, err = openStore(ctx, cfg); err != nil {
			return err
		}
		defer db.Close()
	}

	progress, err := labkit.GroupProgress(ctx, kadm.NewClient(session.Client()), cfg.Input, cfg.group())
	if err != nil {
		return err
	}

	loop := &loop{cfg: cfg, session: session, db: db}
	for !progress.Done() {
		fetches := session.PollRecords(ctx, Batch)
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("%s: read %s with %d left: %w", cfg.Name, cfg.Input, progress.Remaining(), err)
		}
		if err := loop.transact(ctx, fetches); err != nil {
			return err
		}
		fetches.EachRecord(func(record *kgo.Record) { progress.Saw(record.Partition, record.Offset) })
	}
	return nil
}

type loop struct {
	cfg     *Settings
	session *kgo.GroupTransactSession
	db      *pgxpool.Pool
	handled int
}

func (l *loop) transact(ctx context.Context, fetches kgo.Fetches) error {
	if fetches.Empty() {
		return nil
	}
	if err := l.session.Begin(); err != nil {
		return fmt.Errorf("%s: begin: %w", l.cfg.Name, err)
	}

	out, err := l.work(ctx, fetches)
	if err != nil {
		return err
	}
	// Written synchronously so that, if the crash lands next, the records really are on
	// the broker inside an open transaction: that is what a read_uncommitted reader is
	// later handed and a read_committed one is not.
	if err := l.session.ProduceSync(ctx, out...).FirstErr(); err != nil {
		return fmt.Errorf("%s: produce inside the transaction: %w", l.cfg.Name, err)
	}
	if l.cfg.DieAfter > 0 && l.handled >= l.cfg.DieAfter {
		labkit.Crash(l.cfg.Name+": killing the processor mid-transaction", "handled", l.handled)
	}

	// A transaction a rebalance aborted is not an error: the session rolled the offsets
	// back with it, so its records come back on the next poll.
	if _, err := l.session.End(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("%s: end transaction: %w", l.cfg.Name, err)
	}
	return nil
}

func (l *loop) work(ctx context.Context, fetches kgo.Fetches) ([]*kgo.Record, error) {
	var out []*kgo.Record
	var failure error
	fetches.EachRecord(func(record *kgo.Record) {
		if failure != nil {
			return
		}
		var in payment
		if err := json.Unmarshal(record.Value, &in); err != nil {
			failure = fmt.Errorf("%s: undecodable input at offset %d: %w", l.cfg.Name, record.Offset, err)
			return
		}
		if l.db != nil {
			if err := recordPayment(ctx, l.db, l.cfg, in); err != nil {
				failure = err
				return
			}
		}
		out = append(out, &kgo.Record{Topic: l.cfg.Output, Key: record.Key, Value: record.Value})
		l.handled++
	})
	return out, failure
}
