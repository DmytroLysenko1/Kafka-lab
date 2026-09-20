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
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"

	pollBatch = 500
	dbBatch   = 500

	fillerBytes = 60
	maxEvents   = 10_000_000
)

var (
	errMode        = errors.New("exp-01: -mode must be keyed or keyless")
	errDatabase    = errors.New("exp-01: no database, set DATABASE_URL or pass -database-url (make exp-01 does)")
	errShape       = errors.New("exp-01: event count out of range, or not divisible by the payment count")
	errIncomplete  = errors.New("exp-01: the consumer did not see exactly the events this run produced")
	errUndecodable = errors.New("exp-01: undecodable event")
)

// statuses cycle so that a payment's events have a business order, not just a counter.
var statuses = []string{"Requested", "Authorized", "Captured", "Refunded"}

type settings struct {
	mode     string
	brokers  string
	database string
	events   int
	payments int
	timeout  time.Duration
}

// RunID is what keeps a rerun honest: the topic still holds every earlier run, and only
// records carrying this run's id are counted.
type event struct {
	RunID     string `json:"run_id"`
	PaymentID string `json:"payment_id"`
	Seq       int    `json:"seq"`
	Status    string `json:"status,omitempty"`
	Filler    string `json:"filler,omitempty"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-01 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.mode, "mode", "", "keyed (key = payment_id) or keyless (no key)")
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.database, "database-url", os.Getenv("DATABASE_URL"), "where the consumer records what it handled")
	flag.IntVar(&cfg.events, "events", 10_000, "events to produce")
	flag.IntVar(&cfg.payments, "payments", 100, "payments to spread them over")
	flag.DurationVar(&cfg.timeout, "timeout", 3*time.Minute, "deadline for the whole run")
	flag.Parse()

	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	return measure(ctx, cfg, os.Stdout)
}

func (cfg settings) validate() error {
	switch {
	case cfg.mode != "keyed" && cfg.mode != "keyless":
		return errMode
	case cfg.database == "":
		return errDatabase
	case cfg.events <= 0 || cfg.events > maxEvents:
		return fmt.Errorf("%w: -events is %d, wanted 1 to %d", errShape, cfg.events, maxEvents)
	case cfg.payments <= 0 || cfg.events%cfg.payments != 0:
		return fmt.Errorf("%w: %d events over %d payments", errShape, cfg.events, cfg.payments)
	}
	return nil
}

func measure(ctx context.Context, cfg settings, out io.Writer) error {
	topic := "exp01." + cfg.mode
	runID := fmt.Sprintf("%s-%d", cfg.mode, time.Now().UnixNano())

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("exp-01: kafka client: %w", err)
	}
	defer client.Close()

	pool, err := connect(ctx, cfg.database)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := prepare(ctx, pool, cfg.mode); err != nil {
		return err
	}
	if err := produce(ctx, client, topic, runID, cfg); err != nil {
		return err
	}

	if _, err := consume(ctx, client, pool, runID, cfg); err != nil {
		return err
	}

	handled, err := handledOrder(ctx, pool, cfg.mode, runID)
	if err != nil {
		return err
	}
	if len(handled) != cfg.events {
		return fmt.Errorf("exp-01: %w: read back %d of %d handled events", errIncomplete, len(handled), cfg.events)
	}
	return report(out, cfg, handled)
}

// connect builds the pool from a parsed config, so that no error message can carry the
// password out of the connection string and into a log line.
func connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("exp-01: postgres: -database-url is not a valid connection string: %w", errDatabase)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("exp-01: postgres %s:%d/%s: %w",
			config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database, err)
	}
	return pool, nil
}

func produce(ctx context.Context, client *kgo.Client, topic, runID string, cfg settings) error {
	filler := strings.Repeat("x", fillerBytes)

	records := make([]*kgo.Record, 0, cfg.events)
	for i := range cfg.events {
		payment := fmt.Sprintf("pay-%04d", i%cfg.payments)
		seq := i / cfg.payments

		value, err := json.Marshal(event{
			RunID:     runID,
			PaymentID: payment,
			Seq:       seq,
			Status:    statuses[seq%len(statuses)],
			Filler:    filler,
		})
		if err != nil {
			return fmt.Errorf("exp-01: encode event %d: %w", i, err)
		}

		record := &kgo.Record{Topic: topic, Value: value}
		if cfg.mode == "keyed" {
			record.Key = []byte(payment)
		}
		records = append(records, record)
	}

	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("exp-01: produce %d events: %w", cfg.events, err)
	}
	return nil
}

// consume reads from the start of the topic and keeps only what this run produced. The
// topic still holds every earlier run, and counting those would report numbers belonging to
// a different measurement, so the count has to match exactly before anything is reported.
func consume(ctx context.Context, client *kgo.Client, pool *pgxpool.Pool, runID string, cfg settings) ([]processed, error) {
	handled := make([]processed, 0, cfg.events)

	for len(handled) < cfg.events {
		fetches := client.PollRecords(ctx, pollBatch)
		if err := fetches.Err0(); err != nil {
			return nil, fmt.Errorf("exp-01: poll after %d of %d events: %w", len(handled), cfg.events, errors.Join(errIncomplete, err))
		}

		mine, err := ours(fetches, runID)
		if err != nil {
			return nil, err
		}
		if err := store(ctx, pool, cfg.mode, runID, mine); err != nil {
			return nil, err
		}
		handled = append(handled, mine...)
	}

	if len(handled) != cfg.events {
		return nil, fmt.Errorf("exp-01: %w: handled %d, produced %d", errIncomplete, len(handled), cfg.events)
	}
	return handled, nil
}

func ours(fetches kgo.Fetches, runID string) ([]processed, error) {
	mine := make([]processed, 0, dbBatch)

	var failure error
	fetches.EachRecord(func(record *kgo.Record) {
		if failure != nil {
			return
		}

		var decoded event
		if err := json.Unmarshal(record.Value, &decoded); err != nil {
			failure = fmt.Errorf("%w: partition %d offset %d: %w", errUndecodable, record.Partition, record.Offset, err)
			return
		}
		if decoded.RunID != runID {
			return
		}
		mine = append(mine, processed{PaymentID: decoded.PaymentID, Seq: decoded.Seq, Partition: record.Partition})
	})
	return mine, failure
}

func report(out io.Writer, cfg settings, handled []processed) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	rows := [][2]string{
		{"mode", cfg.mode},
		{"produced", fmt.Sprint(cfg.events)},
		{"handled", fmt.Sprint(len(handled))},
		{"payments", fmt.Sprint(cfg.payments)},
		{"payments split across partitions", fmt.Sprint(scatteredPayments(handled))},
		{"order violations", fmt.Sprint(countViolations(handled))},
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(table, "%s\t%s\n", row[0], row[1]); err != nil {
			return fmt.Errorf("exp-01: write report: %w", err)
		}
	}
	return table.Flush()
}
