package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
)

// lines writes the report and keeps the first error rather than checking each line: a
// report that failed halfway is one failure, not twenty.
type lines struct {
	out io.Writer
	err error
}

func (l *lines) printf(format string, args ...any) {
	if l.err != nil {
		return
	}
	_, l.err = fmt.Fprintf(l.out, format, args...)
}

// counts is what the inbox says, split the way the question is asked: the payments that
// had nothing to do with the lock against the ones that were waiting for it.
type counts struct {
	first                        time.Duration
	healthy, hot                 int
	healthyLast, hotLast         time.Duration
	healthyWhileLocked           int
	healthyTotal, hotTotal       int
	totalOfCellsMerchants, inbox int64
}

func measure(ctx context.Context, pool *pgxpool.Pool, s *settings, produced payments, line *timeline) (counts, error) {
	rows, err := pool.Query(ctx, "SELECT event_id, consumed_at FROM inbox WHERE event_id = ANY($1)", produced.all)
	if err != nil {
		return counts{}, fmt.Errorf("read the inbox: %w", err)
	}
	defer rows.Close()

	result := counts{hotTotal: len(produced.hot), healthyTotal: len(produced.all) - len(produced.hot)}
	for rows.Next() {
		var eventID string
		var consumedAt time.Time
		if err := rows.Scan(&eventID, &consumedAt); err != nil {
			return counts{}, fmt.Errorf("scan the inbox: %w", err)
		}
		result.add(produced.hot[eventID], consumedAt, line)
	}
	if err := rows.Err(); err != nil {
		return counts{}, fmt.Errorf("read the inbox: %w", err)
	}

	// Every payment is one minor unit, so the merchants' totals and the inbox must agree to
	// the unit: a payment counted twice shows up here and nowhere else.
	err = pool.QueryRow(ctx,
		"SELECT coalesce(sum(authorized_minor), 0) FROM merchant_totals WHERE merchant_id LIKE $1",
		"exp18-"+s.cell+"-%").Scan(&result.totalOfCellsMerchants)
	if err != nil {
		return counts{}, fmt.Errorf("read the totals: %w", err)
	}
	result.inbox = int64(result.healthy + result.hot)
	return result, nil
}

func (c *counts) add(hot bool, consumedAt time.Time, line *timeline) {
	at := consumedAt.Sub(line.started)
	if c.healthy+c.hot == 0 || at < c.first {
		c.first = at
	}
	if hot {
		c.hot++
		c.hotLast = max(c.hotLast, at)
		return
	}
	c.healthy++
	c.healthyLast = max(c.healthyLast, at)
	if line.released.IsZero() || consumedAt.Before(line.released) {
		c.healthyWhileLocked++
	}
}

func report(ctx context.Context, out io.Writer, pool *pgxpool.Pool, admin *kadm.Client, s *settings, produced payments, line *timeline) error {
	result, err := measure(ctx, pool, s, produced, line)
	if err != nil {
		return err
	}

	written := &lines{out: out}
	written.printf("payments: %d, of them %d for the locked merchant\n", s.payments, len(produced.hot))
	written.printf("first payment counted at: +%s (the group joining and fetching, before any payment is handled)\n", seconds(result.first))
	written.printf("healthy payments counted: %d of %d, the last at +%s\n", result.healthy, result.healthyTotal, seconds(result.healthyLast))
	written.printf("healthy payments counted while the merchant was still locked: %d of %d\n", result.healthyWhileLocked, result.healthyTotal)
	written.printf("contended payments counted: %d of %d, the last at +%s\n", result.hot, result.hotTotal, seconds(result.hotLast))
	written.printf("lock released at: %s\n", since(line.started, line.released))
	written.printf("consumer restarts: %d\n", line.restarts)
	if s.chained() {
		writeChain(ctx, written, admin, s)
	}
	if s.cell == cellExhausted {
		written.printf("replayed: %d dead letters (%d of other classes skipped), finished at %s\n",
			line.replay.Replayed, line.replay.Skipped, since(line.started, line.replayed))
	}
	written.printf("inbox rows: %d, merchants' totals: %d — %s\n", result.inbox, result.totalOfCellsMerchants, agreement(&result))
	if line.drained {
		written.printf("drained\n")
	} else {
		written.printf("never drained within %s\n", s.budget)
	}
	return written.err
}

// writeChain reports how far the contended payments travelled: how many copies entered
// each tier and the dead letter topic. A copy is not a payment — a crash between the copy
// and the commit would make two — which is why the inbox, not these, is the count.
func writeChain(ctx context.Context, written *lines, admin *kadm.Client, s *settings) {
	for _, next := range chainTiers() {
		written.printf("copies into %s: %s\n", s.topic(next.suffix), copies(ctx, admin, s.topic(next.suffix)))
	}
	written.printf("copies into %s: %s\n", s.topic("dlq"), copies(ctx, admin, s.topic("dlq")))
}

func copies(ctx context.Context, admin *kadm.Client, topic string) string {
	n, err := endOffsets(ctx, admin, topic)
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	return fmt.Sprint(n)
}

func agreement(result *counts) string {
	if result.inbox == result.totalOfCellsMerchants {
		return "every counted payment counted once"
	}
	return "DISAGREE: a payment was counted twice or a total moved without an inbox row"
}

func since(start, at time.Time) string {
	if at.IsZero() {
		return "never"
	}
	return "+" + seconds(at.Sub(start))
}

func seconds(d time.Duration) string {
	return d.Round(10 * time.Millisecond).String()
}
