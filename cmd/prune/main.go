// prune deletes what the inbox and the outbox no longer need, then exits. It runs as a
// CronJob on Kubernetes and as `make prune` next to the compose stand.
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/housekeeping"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/retention"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/env"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/lifecycle"
)

const (
	day = 24 * time.Hour
	// 40 days: the longest redelivery (a letter's 30+1 days in the DLQ, then 7+1 in a retry tier
	// after a late replay, each retention plus one daily segment) and a day for clocks that
	// disagree — the least the retention domain accepts.
	defaultInboxHorizon = 40 * day
	// 7 days after publication: payments.main keeps a record that long, and republishing an
	// event older than the topic itself would not help anyone read it.
	defaultOutboxHorizon = 7 * day
	// 5000 rows a statement: at the drills' usual 20 payments/s an hour is 72,000 rows per
	// table, fifteen statements; an hour at 400 payments/s — the highest write rate measured,
	// drill 06 — is 1.44M rows, under three hundred. Each statement's locks and WAL stay
	// short either way.
	defaultBatch = 5000
	// A run that has not finished in ten minutes exits non-zero, so the Job shows as failed:
	// a backlog that large is worth someone looking. Every batch is its own statement, so the
	// deleted rows stay deleted and the next hourly run starts where this one stopped.
	runTimeout = 10 * time.Minute
	pruneConns = 2
)

type config struct {
	databaseURL   string
	inboxHorizon  time.Duration
	outboxHorizon time.Duration
	batch         int
}

func main() { lifecycle.Main("prune", run) }

func run(ctx context.Context, logger *slog.Logger) error {
	settings, err := load()
	if err != nil {
		return err
	}
	horizons, err := retention.NewHorizons(settings.inboxHorizon, settings.outboxHorizon)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	storage, err := postgres.New(ctx, settings.databaseURL, postgres.MaxConns(pruneConns))
	if err != nil {
		return err
	}
	defer storage.Close()

	prune, err := housekeeping.NewPrune(postgres.NewPruneStore(storage), time.Now, horizons, settings.batch)
	if err != nil {
		return err
	}
	result, err := prune.Execute(ctx)
	logger.InfoContext(ctx, "pruned", "inbox_rows", result.Inbox, "outbox_rows", result.Outbox)
	return err
}

func load() (config, error) {
	var read env.Reader
	settings := config{
		databaseURL:   read.Required("DATABASE_URL"),
		inboxHorizon:  read.PositiveDuration("PRUNE_INBOX_HORIZON", defaultInboxHorizon),
		outboxHorizon: read.PositiveDuration("PRUNE_OUTBOX_HORIZON", defaultOutboxHorizon),
		batch:         read.PositiveInt("PRUNE_BATCH", defaultBatch),
	}
	return settings, read.Err()
}
