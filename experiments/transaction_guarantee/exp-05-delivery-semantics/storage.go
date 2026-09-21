package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// No unique key on payment_id on purpose: a duplicate has to be able to land, or exp-06
// would be prevented by the schema from demonstrating the thing it exists to demonstrate.
// The inbox table is where uniqueness lives, and only the inbox mode consults it.
const schema = `
CREATE TABLE IF NOT EXISTS exp05_handled (
	handled_seq bigserial PRIMARY KEY,
	run_id      text    NOT NULL,
	mode        text    NOT NULL,
	payment_id  text    NOT NULL,
	seq         integer NOT NULL
);
CREATE TABLE IF NOT EXISTS exp05_inbox (
	run_id   text NOT NULL,
	mode     text NOT NULL,
	event_id text NOT NULL,
	PRIMARY KEY (run_id, mode, event_id)
)`

type store struct {
	pool *pgxpool.Pool
}

func (s *store) prepare(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("exp-05: create tables: %w", err)
	}
	return nil
}

// record writes the payment with no protection against a repeat. This is at-most-once and
// at-least-once: whether a payment can arrive twice is decided by where the offset is
// committed, and the table must not quietly fix it.
func (s *store) record(ctx context.Context, runID, mode string, handled payment) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO exp05_handled (run_id, mode, payment_id, seq) VALUES ($1, $2, $3, $4)`,
		runID, mode, handled.PaymentID, handled.Seq)
	if err != nil {
		return fmt.Errorf("exp-05: record payment %s: %w", handled.PaymentID, err)
	}
	return nil
}

// recordOnce claims the event and writes the payment in ONE transaction. Claiming first in
// its own statement would be the same bug with more steps: the process can die between the
// claim and the write, and the payment would then be permanently unwritable because the
// claim already exists. Reports whether this call was the one that handled it.
func (s *store) recordOnce(ctx context.Context, runID, mode string, handled payment) (bool, error) {
	claimed := false
	err := s.withinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO exp05_inbox (run_id, mode, event_id) VALUES ($1, $2, $3)
			 ON CONFLICT DO NOTHING`,
			runID, mode, handled.PaymentID)
		if err != nil {
			return fmt.Errorf("exp-05: claim event %s: %w", handled.PaymentID, err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO exp05_handled (run_id, mode, payment_id, seq) VALUES ($1, $2, $3, $4)`,
			runID, mode, handled.PaymentID, handled.Seq); err != nil {
			return fmt.Errorf("exp-05: record claimed payment %s: %w", handled.PaymentID, err)
		}
		claimed = true
		return nil
	})
	return claimed, err
}

func (s *store) withinTx(ctx context.Context, fn func(context.Context, pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("exp-05: begin: %w", err)
	}

	if err := fn(ctx, tx); err != nil {
		// Rollback runs on a context that may already be cancelled, so its own failure is
		// joined onto the real one rather than replacing it.
		return errors.Join(err, ignoreDone(tx.Rollback(ctx)))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("exp-05: commit: %w", err)
	}
	return nil
}

func ignoreDone(err error) error {
	if err == nil || errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return fmt.Errorf("exp-05: rollback: %w", err)
}

// counts reads the result out of the database rather than out of whatever the process
// happened to remember: the process under test is killed halfway through, so its memory is
// not evidence of anything.
func (s *store) counts(ctx context.Context, runID, mode string, produced int) (tally, error) {
	var rows, distinct int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*), count(DISTINCT payment_id) FROM exp05_handled
		 WHERE run_id = $1 AND mode = $2`, runID, mode).Scan(&rows, &distinct)
	if err != nil {
		return tally{}, fmt.Errorf("exp-05: count handled payments: %w", err)
	}
	return tally{Produced: produced, Rows: rows, Distinct: distinct}, nil
}
