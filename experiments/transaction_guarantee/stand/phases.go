package stand

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

// Payment is the event on the topic. Seq is the payment's position in the run, which makes
// a duplicate visible as two rows carrying the same pair rather than as a total.
type Payment struct {
	RunID     string `json:"run_id"`
	PaymentID string `json:"payment_id"`
	Seq       int    `json:"seq"`
}

func measure(ctx context.Context, cfg *Settings, out io.Writer) error {
	pool, err := labkit.Postgres(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	db := &store{
		pool: pool,
	}
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
		value, err := json.Marshal(Payment{
			RunID:     cfg.RunID,
			PaymentID: id,
			Seq:       seq,
		})
		if err != nil {
			return fmt.Errorf("%s: encode payment %s: %w", cfg.Name, id, err)
		}
		records = append(records, &kgo.Record{
			Topic: cfg.Topic,
			Key:   []byte(id),
			Value: value,
		})
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

	lines := []string{
		fmt.Sprintf("%s\tproduced %d, rows %d, distinct %d", cfg.Mode, counted.Produced, counted.Rows, counted.Distinct),
		fmt.Sprintf("  lost\t%d", counted.Lost()),
		fmt.Sprintf("  duplicated\t%d", counted.Duplicated()),
	}
	if cfg.Mode == Inbox {
		lines = append(lines, fmt.Sprintf("  redelivered, refused by the inbox\t%d", counted.Refused))
	}
	verdict, held := cfg.Mode.Verdict(counted)
	lines = append(lines, fmt.Sprintf("  outcome\t%s — %s", counted.Outcome(), verdict))

	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("%s: write report: %w", cfg.Name, err)
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}

	// The report goes out in full first — the numbers are what the run exists to produce —
	// and only then does the process fail. Reporting a refusal and exiting 0 leaves a run
	// that proved nothing looking exactly like one that proved something, and run.sh, which
	// is `set -euo pipefail`, would carry the green all the way to the committed log.
	if !held {
		return fmt.Errorf("%s: %w", cfg.Name, ErrNotDemonstrated)
	}
	return nil
}
