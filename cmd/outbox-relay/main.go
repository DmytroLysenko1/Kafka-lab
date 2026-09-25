package main

import (
	"context"
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

const (
	defaultBatch    = 100
	defaultInterval = time.Second
)

type config struct {
	databaseURL string
	brokers     []string
	registryURL string
	topic       string
	batch       int
	interval    time.Duration
	metricsAddr string
}

func main() { lifecycle.Main("outbox relay", run) }

func run(ctx context.Context, logger *slog.Logger) error {
	settings, err := load()
	if err != nil {
		return err
	}

	storage, err := postgres.New(ctx, settings.databaseURL)
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
	relay := outbox.NewRelay(
		postgres.NewOutboxStore(storage),
		publisher,
		storage,
		time.Now,
		settings.batch,
		logger,
		observed,
	)

	logger.InfoContext(ctx, "outbox relay started",
		"topic", settings.topic,
		"batch", settings.batch,
		"interval", settings.interval.String(),
		"metrics", settings.metricsAddr,
	)

	// The relay and its metrics stop together: a scrape endpoint that outlived the work it
	// reports on would answer a health check for a service that is no longer running.
	running, stop := errgroup.WithContext(ctx)
	running.Go(func() error { return relay.Run(stop, settings.interval) })
	running.Go(func() error { return metrics.Serve(stop, settings.metricsAddr, observed.Gatherer()) })
	return running.Wait()
}

func load() (config, error) {
	var read env.Reader
	settings := config{
		databaseURL: read.Required("DATABASE_URL"),
		brokers:     read.List("KAFKA_BROKERS"),
		registryURL: read.Required("SCHEMA_REGISTRY_URL"),
		topic:       env.Or("OUTBOX_TOPIC", "payments.main"),
		batch:       read.PositiveInt("OUTBOX_BATCH", defaultBatch),
		interval:    read.PositiveDuration("OUTBOX_INTERVAL", defaultInterval),
		metricsAddr: env.Or("METRICS_ADDR", "127.0.0.1:9101"),
	}
	return settings, read.Err()
}
