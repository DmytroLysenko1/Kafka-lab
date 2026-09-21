package eos

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

// No unique key on payment_id, on purpose: this table stands for any side effect the Kafka
// transaction cannot reach, and a constraint here would be the idempotency the experiment
// is showing the transaction does not provide.
const schema = `
CREATE TABLE IF NOT EXISTS eos_handled (
	handled_seq bigserial PRIMARY KEY,
	experiment  text NOT NULL,
	run_id      text NOT NULL,
	payment_id  text NOT NULL
)`

func openStore(ctx context.Context, cfg *Settings) (*pgxpool.Pool, error) {
	pool, err := labkit.Postgres(ctx, cfg.Database)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("%s: create table: %w", cfg.Name, err)
	}
	return pool, nil
}

func recordPayment(ctx context.Context, db *pgxpool.Pool, cfg *Settings, handled payment) error {
	_, err := db.Exec(ctx,
		`INSERT INTO eos_handled (experiment, run_id, payment_id) VALUES ($1, $2, $3)`,
		cfg.Name, cfg.RunID, handled.PaymentID)
	if err != nil {
		return fmt.Errorf("%s: record payment %s: %w", cfg.Name, handled.PaymentID, err)
	}
	return nil
}

func dbCounts(ctx context.Context, db *pgxpool.Pool, cfg *Settings) (int, int, error) {
	var rows, distinct int
	err := db.QueryRow(ctx,
		`SELECT count(*), count(DISTINCT payment_id) FROM eos_handled WHERE experiment = $1 AND run_id = $2`,
		cfg.Name, cfg.RunID).Scan(&rows, &distinct)
	if err != nil {
		return 0, 0, fmt.Errorf("%s: count database rows: %w", cfg.Name, err)
	}
	return rows, distinct, nil
}
