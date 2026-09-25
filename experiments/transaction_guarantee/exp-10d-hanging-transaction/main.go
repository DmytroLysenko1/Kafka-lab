package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	topic          = "exp10d.stream"
	maxRecords     = 100_000
	openMarker     = "open-transaction"
)

var (
	errShape  = errors.New("exp-10d: -records or -transaction-timeout out of range")
	errRunID  = errors.New("exp-10d: -run-id is required")
	errUnseen = errors.New("exp-10d: the records never became visible inside the deadline")
)

type settings struct {
	brokers    string
	runID      string
	records    int
	txnTimeout time.Duration
	timeout    time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-10d failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.runID, "run-id", "", "identifier for this run")
	flag.IntVar(&cfg.records, "records", 100, "records an unrelated producer writes after the transaction opens")
	flag.DurationVar(&cfg.txnTimeout, "transaction-timeout", 20*time.Second, "transaction.timeout.ms of the stuck producer")
	flag.DurationVar(&cfg.timeout, "timeout", 3*time.Minute, "deadline for the whole run")
	flag.Parse()

	switch {
	case cfg.runID == "":
		return errRunID
	case cfg.records <= 0 || cfg.records > maxRecords || cfg.txnTimeout < time.Second || cfg.txnTimeout > cfg.timeout:
		return fmt.Errorf("%w: -records %d, -transaction-timeout %s", errShape, cfg.records, cfg.txnTimeout)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	return measure(ctx, &cfg, os.Stdout)
}

func (cfg *settings) seeds() []string { return strings.Split(cfg.brokers, ",") }

// measure opens a transaction and leaves it open, lets an unrelated producer write after
// it, and times how long each isolation level takes to see the unrelated records. The stuck
// producer never ends its transaction: the coordinator aborts it at transaction.timeout.ms,
// which is the only thing that ever releases the readers behind it.
func measure(ctx context.Context, cfg *settings, out io.Writer) error {
	stuck, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.seeds()...),
		kgo.TransactionalID("exp-10d-"+cfg.runID),
		kgo.TransactionTimeout(cfg.txnTimeout),
	)
	if err != nil {
		return fmt.Errorf("exp-10d: transactional client: %w", err)
	}
	defer stuck.Close()

	if err := openAndAbandon(ctx, stuck, cfg); err != nil {
		return err
	}
	producedAt, err := writeUnrelated(ctx, cfg)
	if err != nil {
		return err
	}
	stable, newest, err := stableAndNewest(ctx, cfg)
	if err != nil {
		return err
	}

	uncommitted, err := timeToSee(ctx, cfg, kgo.ReadUncommitted(), producedAt)
	if err != nil {
		return err
	}
	committed, err := timeToSee(ctx, cfg, kgo.ReadCommitted(), producedAt)
	if err != nil {
		return err
	}
	return render(out, report(cfg, stall{
		Stable: stable, Newest: newest, Uncommitted: uncommitted, Committed: committed,
	}))
}

// openAndAbandon writes one record inside a transaction and does not end it — what a
// producer that hangs, deadlocks or loses its thread after producing leaves behind.
func openAndAbandon(ctx context.Context, stuck *kgo.Client, cfg *settings) error {
	if err := stuck.BeginTransaction(); err != nil {
		return fmt.Errorf("exp-10d: begin: %w", err)
	}
	record := &kgo.Record{Topic: topic, Key: []byte(cfg.runID), Value: []byte(openMarker)}
	if err := stuck.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("exp-10d: produce inside the transaction: %w", err)
	}
	return nil
}

// writeUnrelated is a different producer, not transactional, writing after the stuck
// transaction began. Nothing about these records is uncommitted — they are held back only
// because they sit behind a transaction that is.
func writeUnrelated(ctx context.Context, cfg *settings) (time.Time, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.seeds()...))
	if err != nil {
		return time.Time{}, fmt.Errorf("exp-10d: plain client: %w", err)
	}
	defer client.Close()

	records := make([]*kgo.Record, 0, cfg.records)
	for i := range cfg.records {
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(cfg.runID), Value: fmt.Appendf(nil, "unrelated-%d", i)})
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return time.Time{}, fmt.Errorf("exp-10d: produce the unrelated records: %w", err)
	}
	return time.Now(), nil
}

// stableAndNewest is the symptom an operator actually sees: the high watermark has moved
// and the last stable offset has not, so lag is reported and nothing arrives.
func stableAndNewest(ctx context.Context, cfg *settings) (int64, int64, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.seeds()...))
	if err != nil {
		return 0, 0, fmt.Errorf("exp-10d: admin client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	lso, err := admin.ListCommittedOffsets(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("exp-10d: last stable offset: %w", err)
	}
	hw, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("exp-10d: high watermark: %w", err)
	}
	stable, err := labkit.OffsetAt(lso, topic, 0)
	if err != nil {
		return 0, 0, fmt.Errorf("exp-10d: %w", err)
	}
	newest, err := labkit.OffsetAt(hw, topic, 0)
	if err != nil {
		return 0, 0, fmt.Errorf("exp-10d: %w", err)
	}
	return stable, newest, nil
}

// timeToSee reads from the start with the given isolation level and reports how long after
// the unrelated records were written it took to be handed all of them.
func timeToSee(ctx context.Context, cfg *settings, isolation kgo.IsolationLevel, since time.Time) (time.Duration, error) {
	reader, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.seeds()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(isolation),
	)
	if err != nil {
		return 0, fmt.Errorf("exp-10d: reader: %w", err)
	}
	defer reader.Close()

	seen := 0
	for seen < cfg.records {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return 0, fmt.Errorf("%w: %d of %d seen: %w", errUnseen, seen, cfg.records, err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if strings.HasPrefix(string(r.Value), "unrelated-") && string(r.Key) == cfg.runID {
				seen++
			}
		})
	}
	return time.Since(since), nil
}

func render(out io.Writer, lines []string) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("exp-10d: write report: %w", err)
		}
	}
	return table.Flush()
}
