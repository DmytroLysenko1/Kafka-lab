package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// handled_seq is what makes this table an experiment rather than a log: it records the
// order the consumer handled events in, which is the order the business sees. Offsets
// only order records inside one partition.
const schema = `
CREATE TABLE IF NOT EXISTS exp01_events (
	handled_seq bigserial PRIMARY KEY,
	run         text    NOT NULL,
	run_id      text    NOT NULL,
	payment_id  text    NOT NULL,
	seq         integer NOT NULL,
	partition   integer NOT NULL
)`

// prepare makes a rerun mean the same as a first run: the previous rows of this mode are
// dropped before anything is produced.
func prepare(ctx context.Context, pool *pgxpool.Pool, mode string) error {
	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("exp-01: create table: %w", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM exp01_events WHERE run = $1`, mode); err != nil {
		return fmt.Errorf("exp-01: clear previous %s run: %w", mode, err)
	}
	return nil
}

func store(ctx context.Context, pool *pgxpool.Pool, mode, runID string, events []processed) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, event := range events {
		batch.Queue(
			`INSERT INTO exp01_events (run, run_id, payment_id, seq, partition) VALUES ($1, $2, $3, $4, $5)`,
			mode, runID, event.PaymentID, event.Seq, event.Partition,
		)
	}

	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("exp-01: record %d handled events: %w", len(events), err)
	}
	return nil
}
