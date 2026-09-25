package stand

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// No unique key on payment_id on purpose: a duplicate has to be able to land, or exp-06
// would be prevented by the schema from demonstrating the thing it exists to demonstrate.
// The inbox table is where uniqueness lives, and only exp-07 consults it. delivery_refused
// is exp-07's evidence: without a count of the redeliveries the inbox turned away, a clean
// table cannot be told apart from a run in which nothing was redelivered at all.
const schema = `
CREATE TABLE IF NOT EXISTS delivery_handled (
	handled_seq bigserial PRIMARY KEY,
	run_id      text    NOT NULL,
	mode        text    NOT NULL,
	payment_id  text    NOT NULL,
	seq         integer NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_inbox (
	run_id   text NOT NULL,
	mode     text NOT NULL,
	event_id text NOT NULL,
	PRIMARY KEY (run_id, mode, event_id)
);
CREATE TABLE IF NOT EXISTS delivery_refused (
	refused_seq bigserial PRIMARY KEY,
	run_id      text NOT NULL,
	mode        text NOT NULL,
	payment_id  text NOT NULL
)`

type store struct {
	pool *pgxpool.Pool
}

func (s *store) prepare(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("stand: create tables: %w", err)
	}
	return nil
}

// record writes the payment with no protection against a repeat. This is at-most-once and
// at-least-once: whether a payment can arrive twice is decided by where the offset is
// committed, and the table must not quietly fix it.
func (s *store) record(ctx context.Context, runID string, mode Mode, handled Payment) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO delivery_handled (run_id, mode, payment_id, seq) VALUES ($1, $2, $3, $4)`,
		runID, mode, handled.PaymentID, handled.Seq)
	if err != nil {
		return fmt.Errorf("stand: record payment %s: %w", handled.PaymentID, err)
	}
	return nil
}

// recordOnce claims the event and writes the payment in ONE transaction. Claiming first in
// its own statement would be the same bug with more steps: the process can die between the
// claim and the write, and the payment would then be permanently unwritable because the
// claim already exists. A claim that is already taken is recorded as refused.
func (s *store) recordOnce(ctx context.Context, runID string, mode Mode, handled Payment) error {
	return s.withinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO delivery_inbox (run_id, mode, event_id) VALUES ($1, $2, $3)
			 ON CONFLICT DO NOTHING`,
			runID, mode, handled.PaymentID)
		if err != nil {
			return fmt.Errorf("stand: claim event %s: %w", handled.PaymentID, err)
		}
		if tag.RowsAffected() == 0 {
			return refuse(ctx, tx, runID, mode, handled)
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO delivery_handled (run_id, mode, payment_id, seq) VALUES ($1, $2, $3, $4)`,
			runID, mode, handled.PaymentID, handled.Seq); err != nil {
			return fmt.Errorf("stand: record claimed payment %s: %w", handled.PaymentID, err)
		}
		return nil
	})
}

// refuse records a redelivery the inbox turned away, in the same transaction as the claim
// that found it taken.
func refuse(ctx context.Context, tx pgx.Tx, runID string, mode Mode, handled Payment) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO delivery_refused (run_id, mode, payment_id) VALUES ($1, $2, $3)`,
		runID, mode, handled.PaymentID); err != nil {
		return fmt.Errorf("stand: record refused redelivery %s: %w", handled.PaymentID, err)
	}
	return nil
}

func (s *store) withinTx(ctx context.Context, fn func(context.Context, pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("stand: begin: %w", err)
	}

	if err := fn(ctx, tx); err != nil {
		// Rollback runs on a context that may already be cancelled, so its own failure is
		// joined onto the real one rather than replacing it.
		return errors.Join(err, ignoreDone(tx.Rollback(ctx)))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("stand: commit: %w", err)
	}
	return nil
}

func ignoreDone(err error) error {
	if err == nil || errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return fmt.Errorf("stand: rollback: %w", err)
}

// counts reads the result out of the database rather than out of whatever the process
// happened to remember: the process under test is killed halfway through, so its memory is
// not evidence of anything.
func (s *store) counts(ctx context.Context, runID string, mode Mode, produced int) (Tally, error) {
	var rows, distinct, refused int
	err := s.pool.QueryRow(ctx,
		`SELECT
		   (SELECT count(*) FROM delivery_handled WHERE run_id = $1 AND mode = $2),
		   (SELECT count(DISTINCT payment_id) FROM delivery_handled WHERE run_id = $1 AND mode = $2),
		   (SELECT count(*) FROM delivery_refused WHERE run_id = $1 AND mode = $2)`,
		runID, mode).Scan(&rows, &distinct, &refused)
	if err != nil {
		return Tally{}, fmt.Errorf("stand: count handled payments: %w", err)
	}
	return Tally{
		Produced: produced,
		Rows:     rows,
		Distinct: distinct,
		Refused:  refused,
	}, nil
}
