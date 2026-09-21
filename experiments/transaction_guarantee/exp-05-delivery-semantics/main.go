package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	topic          = "exp05.payments"

	maxPayments = 1_000_000
	pollBatch   = 50
)

const (
	modeAtMostOnce  = "at-most-once"
	modeAtLeastOnce = "at-least-once"
	modeInbox       = "inbox"
)

var modes = []string{modeAtMostOnce, modeAtLeastOnce, modeInbox}

var phases = []string{"produce", "consume", "verify"}

var (
	errMode     = errors.New("exp-05: -mode must be at-most-once, at-least-once or inbox")
	errPhase    = errors.New("exp-05: -phase must be produce, consume or verify")
	errRunID    = errors.New("exp-05: -run-id is required, so a rerun cannot read an earlier run's rows")
	errShape    = errors.New("exp-05: -payments out of range")
	errDatabase = errors.New("exp-05: postgres")
	errPartial  = errors.New("exp-05: the cluster did not accept every payment")
)

type settings struct {
	brokers  string
	database string
	mode     string
	phase    string
	runID    string
	payments int
	dieAfter int
	timeout  time.Duration
}

// payment is the event on the topic. seq is the payment's position in the run, which makes
// a duplicate visible as two rows carrying the same pair rather than as a total.
type payment struct {
	RunID     string `json:"run_id"`
	PaymentID string `json:"payment_id"`
	Seq       int    `json:"seq"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-05 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.database, "database-url", os.Getenv("DATABASE_URL"), "postgres connection string")
	flag.StringVar(&cfg.mode, "mode", "", strings.Join(modes, ", "))
	flag.StringVar(&cfg.phase, "phase", "", strings.Join(phases, ", "))
	flag.StringVar(&cfg.runID, "run-id", "", "identifier shared by the phases of one run")
	flag.IntVar(&cfg.payments, "payments", 1000, "payments produced in this run")
	flag.IntVar(&cfg.dieAfter, "die-after", 0, "SIGKILL this process once it has handled this many payments; 0 never dies")
	flag.DurationVar(&cfg.timeout, "timeout", 3*time.Minute, "deadline for this phase")
	flag.Parse()

	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	return measure(ctx, &cfg, os.Stdout)
}

func (cfg *settings) validate() error {
	switch {
	case !slices.Contains(modes, cfg.mode):
		return errMode
	case !slices.Contains(phases, cfg.phase):
		return errPhase
	case cfg.runID == "":
		return errRunID
	case cfg.payments <= 0 || cfg.payments > maxPayments:
		return fmt.Errorf("%w: -payments is %d", errShape, cfg.payments)
	case cfg.dieAfter < 0 || cfg.dieAfter > cfg.payments:
		return fmt.Errorf("%w: -die-after is %d", errShape, cfg.dieAfter)
	}
	return nil
}

func measure(ctx context.Context, cfg *settings, out io.Writer) error {
	pool, err := connect(ctx, cfg.database)
	if err != nil {
		return err
	}
	defer pool.Close()

	db := &store{pool: pool}
	if err := db.prepare(ctx); err != nil {
		return err
	}

	switch cfg.phase {
	case "produce":
		return produce(ctx, cfg)
	case "consume":
		return consume(ctx, cfg, db)
	default:
		return verify(ctx, cfg, db, out)
	}
}

// connect builds the pool from a parsed config, so that no error message can carry the
// password out of the connection string and into a log line.
func connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("%w: -database-url is not a valid connection string", errDatabase)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("%w: connect to %s:%d/%s", errDatabase,
			config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("%w: ping %s:%d/%s", errDatabase,
			config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database)
	}
	return pool, nil
}

func produce(ctx context.Context, cfg *settings) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("exp-05: kafka client: %w", err)
	}
	defer client.Close()

	records := make([]*kgo.Record, 0, cfg.payments)
	for seq := range cfg.payments {
		id := fmt.Sprintf("%s-%05d", cfg.runID, seq)
		value, err := json.Marshal(payment{RunID: cfg.runID, PaymentID: id, Seq: seq})
		if err != nil {
			return fmt.Errorf("exp-05: encode payment %s: %w", id, err)
		}
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(id), Value: value})
	}

	// Anything less than every payment accepted and the run is measuring the producer, not
	// the consumer's commit order.
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("%w: %d intended: %w", errPartial, cfg.payments, err)
	}
	return nil
}

// consume reads the run and writes it to Postgres. Which of the three semantics it
// demonstrates is decided entirely by where CommitRecords sits relative to the write.
func consume(ctx context.Context, cfg *settings, db *store) error {
	group := fmt.Sprintf("exp05-%s-%s", cfg.mode, cfg.runID)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ConsumeTopics(topic),
		// One group per run and mode, so a restarted process resumes where the killed one
		// committed — which is the whole mechanism under test — while a later run starts
		// from nothing.
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("exp-05: kafka client: %w", err)
	}
	defer client.Close()

	drained, err := endOffsets(ctx, cfg, group)
	if err != nil {
		return err
	}

	run := &consumer{cfg: cfg, db: db, client: client}
	for !drained.reached() {
		fetches := client.PollRecords(ctx, pollBatch)
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("exp-05: read %s with %d left: %w", topic, drained.remaining(), err)
		}

		batch, err := decode(fetches, cfg.runID)
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
		return fmt.Errorf("exp-05: commit final offsets: %w", err)
	}
	return nil
}

type consumer struct {
	cfg     *settings
	db      *store
	client  *kgo.Client
	handled int
}

// commitFirst is the whole difference between at-most-once and the other two: storing the
// offset before the payment is written means a crash in between loses the payment, and
// storing it after means the crash replays it.
func commitFirst(mode string) bool {
	return mode == modeAtMostOnce
}

func (c *consumer) apply(ctx context.Context, fetches kgo.Fetches, batch []payment) error {
	if commitFirst(c.cfg.mode) {
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

func (c *consumer) writeAll(ctx context.Context, batch []payment) error {
	for i, handledPayment := range batch {
		if err := c.write(ctx, handledPayment); err != nil {
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
	if c.cfg.dieAfter <= 0 || c.handled < c.cfg.dieAfter {
		return false
	}
	return !commitFirst(c.cfg.mode) || !lastOfBatch
}

func (c *consumer) write(ctx context.Context, handledPayment payment) error {
	if c.cfg.mode != modeInbox {
		return c.db.record(ctx, c.cfg.runID, c.cfg.mode, handledPayment)
	}
	_, err := c.db.recordOnce(ctx, c.cfg.runID, c.cfg.mode, handledPayment)
	return err
}

func (c *consumer) commit(ctx context.Context, fetches kgo.Fetches) error {
	if err := c.client.CommitRecords(ctx, records(fetches)...); err != nil {
		return fmt.Errorf("exp-05: commit offsets for %s: %w", c.cfg.mode, err)
	}
	return nil
}

// die kills this process the way a crash would. SIGKILL cannot be caught, so nothing here
// tidies up: that is the point, and it is why the kill is delivered rather than returned.
func die(cfg *settings, handled int) {
	slog.Warn("exp-05: killing this consumer", "mode", cfg.mode, "handled", handled)
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		os.Exit(137)
	}
	select {} // unreachable: SIGKILL is delivered before the next statement runs
}

func decode(fetches kgo.Fetches, runID string) ([]payment, error) {
	batch := make([]payment, 0, pollBatch)

	var failure error
	fetches.EachRecord(func(record *kgo.Record) {
		if failure != nil {
			return
		}
		var decoded payment
		if err := json.Unmarshal(record.Value, &decoded); err != nil {
			failure = fmt.Errorf("exp-05: undecodable record at %s offset %d: %w", topic, record.Offset, err)
			return
		}
		if decoded.RunID != runID {
			return
		}
		batch = append(batch, decoded)
	})
	return batch, failure
}

func records(fetches kgo.Fetches) []*kgo.Record {
	all := make([]*kgo.Record, 0, pollBatch)
	fetches.EachRecord(func(record *kgo.Record) { all = append(all, record) })
	return all
}

func verify(ctx context.Context, cfg *settings, db *store, out io.Writer) error {
	counted, err := db.counts(ctx, cfg.runID, cfg.mode, cfg.payments)
	if err != nil {
		return err
	}

	got := counted.outcome()
	note := "as expected"
	if want := expected[cfg.mode]; got != want {
		note = fmt.Sprintf("EXPECTED %q — this run did not demonstrate what it exists to demonstrate", want)
	}

	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	lines := []string{
		fmt.Sprintf("%s\tproduced %d, rows %d, distinct %d", cfg.mode, counted.Produced, counted.Rows, counted.Distinct),
		fmt.Sprintf("  lost\t%d", counted.Lost()),
		fmt.Sprintf("  duplicated\t%d", counted.Duplicated()),
		fmt.Sprintf("  outcome\t%s — %s", got, note),
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("exp-05: write report: %w", err)
		}
	}
	return table.Flush()
}
