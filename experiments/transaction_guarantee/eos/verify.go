package eos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

// view is one way of reading the result: how many rows, how many distinct payments.
type view struct {
	Rows     int
	Distinct int
}

func (v view) duplicated() int { return max(v.Rows-v.Distinct, 0) }

func (v view) exact(produced int) bool { return v.Rows == produced && v.Distinct == produced }

// views is everything the three experiments can be judged on.
type views struct {
	Produced    int
	Committed   view
	Uncommitted view
	Database    view
}

// judge holds a run to its experiment's claim. Each claim has two halves: the thing Kafka's
// transaction is supposed to guarantee, and the evidence that the run gave it something to
// guarantee — for 10a an aborted batch still sitting in the log, for 10b the database rows it
// did not reach, for 10c the aborted records a read_uncommitted reader is handed. A run
// satisfying only the first half would read as a success while demonstrating nothing: one
// whose crash never landed mid-transaction is exact too. So both halves are required.
func judge(claim Claim, v views) (bool, string) {
	kafkaExact := v.Committed.exact(v.Produced)
	switch claim {
	case DatabaseNotCovered:
		return kafkaExact && v.Database.duplicated() > 0,
			"the Kafka output exactly once, and the database rows duplicated"
	case AbortedVisibleUncommitted:
		return kafkaExact && v.Uncommitted.duplicated() > 0,
			"read_committed exactly once, and read_uncommitted handed the aborted records"
	default:
		return kafkaExact && v.Uncommitted.duplicated() > 0,
			"read_committed exactly once, although a batch was aborted mid-transaction"
	}
}

func verify(ctx context.Context, cfg *Settings, out io.Writer) error {
	committed, err := readView(ctx, cfg, kgo.ReadCommitted())
	if err != nil {
		return err
	}
	uncommitted, err := readView(ctx, cfg, kgo.ReadUncommitted())
	if err != nil {
		return err
	}
	got := views{Produced: cfg.Payments, Committed: committed, Uncommitted: uncommitted}

	if cfg.WriteDB {
		db, err := openStore(ctx, cfg)
		if err != nil {
			return err
		}
		defer db.Close()
		if got.Database.Rows, got.Database.Distinct, err = dbCounts(ctx, db, cfg); err != nil {
			return err
		}
	}
	return render(out, report(cfg, got))
}

func readView(ctx context.Context, cfg *Settings, isolation kgo.IsolationLevel) (view, error) {
	records, err := labkit.ReadAll(ctx, cfg.brokers(), cfg.Output, readStall, kgo.FetchIsolationLevel(isolation))
	if err != nil {
		return view{}, err
	}

	seen := make(map[string]bool, len(records))
	var v view
	for _, r := range records {
		var p payment
		if err := json.Unmarshal(r.Value, &p); err != nil {
			return view{}, fmt.Errorf("%s: undecodable output at offset %d: %w", cfg.Name, r.Offset, err)
		}
		if p.RunID != cfg.RunID {
			continue
		}
		v.Rows++
		seen[p.PaymentID] = true
	}
	v.Distinct = len(seen)
	return v, nil
}

func report(cfg *Settings, v views) []string {
	held, claim := judge(cfg.Claim, v)
	verdict := "as expected"
	if !held {
		verdict = "NOT DEMONSTRATED — this run did not show what it exists to show"
	}

	lines := []string{
		fmt.Sprintf("payments\t%d seeded", v.Produced),
		fmt.Sprintf("read_committed\t%d rows, %d distinct, %d duplicated", v.Committed.Rows, v.Committed.Distinct, v.Committed.duplicated()),
		fmt.Sprintf("read_uncommitted\t%d rows, %d distinct, %d duplicated", v.Uncommitted.Rows, v.Uncommitted.Distinct, v.Uncommitted.duplicated()),
	}
	if cfg.WriteDB {
		lines = append(lines, fmt.Sprintf("postgres\t%d rows, %d distinct, %d duplicated", v.Database.Rows, v.Database.Distinct, v.Database.duplicated()))
	}
	return append(lines, fmt.Sprintf("claim\t%s — %s", claim, verdict))
}

func render(out io.Writer, lines []string) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("eos: write report: %w", err)
		}
	}
	return table.Flush()
}
