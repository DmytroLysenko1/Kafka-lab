package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/env"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/lifecycle"
)

// A sweep claims up to defaultBatch rows; when it comes back full the next starts at once, so
// under load the relay publishes batches back to back — drill 06 cleared 400 payments/s with
// these values — and defaultInterval is only the delay of an idle stand: an event waits at most
// a second to be published. Polling rather than LISTEN/NOTIFY or CDC keeps the relay a plain
// loop over one query (docs/patterns.md, CDC).
const (
	defaultBatch    = 100
	defaultInterval = time.Second
)

var errBatchPastClaim = errors.New("outbox relay: OUTBOX_BATCH is larger than one claim may lock")

// The relay's waits. A sweep's publish is bounded by the publisher (10 s per request) and its
// commit by the store (5 s); sweepTimeout is the outer fence around the pair, so a database
// that stops answering cannot hold the claimed rows past it. The backlog is a dashboard number:
// a count that hangs must not freeze the gauge for longer than backlogTimeout.
const (
	sweepTimeout   = 30 * time.Second
	backlogTimeout = 2 * time.Second
)

type config struct {
	databaseURL string
	maxConns    int32
	brokers     []string
	registryURL string
	topic       string
	batch       int
	interval    time.Duration
	metricsAddr string
}

func main() { lifecycle.Main("outbox-relay", run) }

func run(ctx context.Context, logger *slog.Logger) error {
	settings, err := load()
	if err != nil {
		return err
	}

	storage, err := postgres.New(ctx, settings.databaseURL, postgres.MaxConns(settings.maxConns))
	if err != nil {
		return err
	}
	defer storage.Close()

	publisher, err := kafka.NewPublisher(ctx, settings.brokers, settings.topic, settings.registryURL)
	if err != nil {
		return err
	}
	defer publisher.Close()

	observed := metrics.New()
	relay, err := provideRelay(storage, publisher, &settings, logger, observed)
	if err != nil {
		return err
	}

	logger.InfoContext(ctx, "outbox relay started",
		"topic", settings.topic,
		"batch", settings.batch,
		"interval", settings.interval.String(),
		"metrics", settings.metricsAddr,
	)
	return serve(ctx, &settings, relay, observed)
}

func provideRelay(storage *postgres.Storage, publisher *kafka.Publisher, settings *config, logger *slog.Logger, observed *metrics.Registry) (*outbox.Relay, error) {
	return outbox.NewRelay(
		postgres.NewOutboxStore(storage),
		publisher,
		postgres.NewOutboxTransactions(storage),
		time.Now,
		outbox.Limits{
			Batch:          settings.batch,
			SweepTimeout:   sweepTimeout,
			BacklogTimeout: backlogTimeout,
		},
		logger,
		observed,
	)
}

// serve runs the relay and its metrics until either stops: a scrape endpoint that outlived the
// work it reports on would answer a health check for a service that is no longer running.
func serve(ctx context.Context, settings *config, relay *outbox.Relay, observed *metrics.Registry) error {
	running, stop := errgroup.WithContext(ctx)
	running.Go(func() error { return relay.Run(stop, settings.interval) })
	running.Go(func() error { return metrics.Serve(stop, settings.metricsAddr, observed.Gatherer()) })
	return running.Wait()
}

func load() (config, error) {
	var read env.Reader
	settings := config{
		databaseURL: read.Required("DATABASE_URL"),
		// One for the sweep's transaction, one for the backlog count that runs on its own
		// clock. The budget across services is in deploy/helm/payments/values.yaml.
		maxConns:    read.PositiveInt32("DB_MAX_CONNS", 2),
		brokers:     read.List("KAFKA_BROKERS"),
		registryURL: read.Required("SCHEMA_REGISTRY_URL"),
		topic:       env.Or("OUTBOX_TOPIC", "payments.main"),
		batch:       read.PositiveInt("OUTBOX_BATCH", defaultBatch),
		interval:    read.PositiveDuration("OUTBOX_INTERVAL", defaultInterval),
		metricsAddr: env.Or("METRICS_ADDR", "127.0.0.1:9101"),
	}
	if err := read.Err(); err != nil {
		return config{}, err
	}
	if settings.batch > postgres.MaxClaim {
		return config{}, fmt.Errorf("%w: %d > %d", errBatchPastClaim, settings.batch, postgres.MaxClaim)
	}
	return settings, nil
}
