package stand

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Payment is the event on the topic. Seq is the payment's position in the run, which makes
// a duplicate visible as two rows carrying the same pair rather than as a total.
type Payment struct {
	RunID     string `json:"run_id"`
	PaymentID string `json:"payment_id"`
	Seq       int    `json:"seq"`
}

func measure(ctx context.Context, cfg *Settings, out io.Writer) error {
	pool, err := connect(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	db := &store{pool: pool}
	if err := db.prepare(ctx); err != nil {
		return err
	}

	switch cfg.Phase {
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
		return nil, fmt.Errorf("%w: -database-url is not a valid connection string", ErrDatabase)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("%w: connect to %s:%d/%s", ErrDatabase,
			config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("%w: ping %s:%d/%s", ErrDatabase,
			config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database)
	}
	return pool, nil
}

func produce(ctx context.Context, cfg *Settings) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.Brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("%s: kafka client: %w", cfg.Name, err)
	}
	defer client.Close()

	records := make([]*kgo.Record, 0, cfg.Payments)
	for seq := range cfg.Payments {
		id := fmt.Sprintf("%s-%05d", cfg.RunID, seq)
		value, err := json.Marshal(Payment{RunID: cfg.RunID, PaymentID: id, Seq: seq})
		if err != nil {
			return fmt.Errorf("%s: encode payment %s: %w", cfg.Name, id, err)
		}
		records = append(records, &kgo.Record{Topic: cfg.Topic, Key: []byte(id), Value: value})
	}

	// Anything less than every payment accepted and the run is measuring the producer, not
	// the consumer's commit order.
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("%w: %d intended: %w", ErrPartial, cfg.Payments, err)
	}
	return nil
}

func verify(ctx context.Context, cfg *Settings, db *store, out io.Writer) error {
	counted, err := db.counts(ctx, cfg.RunID, cfg.Mode, cfg.Payments)
	if err != nil {
		return err
	}

	got := counted.Outcome()
	note := "as expected"
	if want := cfg.Mode.Expects(); got != want {
		note = fmt.Sprintf("EXPECTED %q — this run did not demonstrate what it exists to demonstrate", want)
	}

	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	lines := []string{
		fmt.Sprintf("%s\tproduced %d, rows %d, distinct %d", cfg.Mode, counted.Produced, counted.Rows, counted.Distinct),
		fmt.Sprintf("  lost\t%d", counted.Lost()),
		fmt.Sprintf("  duplicated\t%d", counted.Duplicated()),
		fmt.Sprintf("  outcome\t%s — %s", got, note),
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("%s: write report: %w", cfg.Name, err)
		}
	}
	return table.Flush()
}
