package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
)

var errMissing = errors.New("outbox-relay: required configuration is missing")

type config struct {
	databaseURL string
	brokers     []string
	registryURL string
	topic       string
	batch       int
	interval    time.Duration
	metricsAddr string
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	// os.Exit skips deferred calls, so it stays out of the function that holds them: the
	// signal handler and the connections must be released before the process ends.
	if code := start(logger); code != 0 {
		os.Exit(code)
	}
}

func start(logger *slog.Logger) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger); err != nil {
		logger.ErrorContext(ctx, "outbox relay stopped", "error", err)
		return 1
	}
	logger.InfoContext(ctx, "outbox relay stopped")
	return 0
}

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
	settings := config{
		databaseURL: os.Getenv("DATABASE_URL"),
		registryURL: os.Getenv("SCHEMA_REGISTRY_URL"),
		topic:       envOr("OUTBOX_TOPIC", "payments.main"),
		metricsAddr: envOr("METRICS_ADDR", "127.0.0.1:9101"),
	}
	for _, broker := range strings.Split(os.Getenv("KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			settings.brokers = append(settings.brokers, broker)
		}
	}

	switch {
	case settings.databaseURL == "":
		return config{}, fmt.Errorf("%w: DATABASE_URL", errMissing)
	case len(settings.brokers) == 0:
		return config{}, fmt.Errorf("%w: KAFKA_BROKERS", errMissing)
	case settings.registryURL == "":
		return config{}, fmt.Errorf("%w: SCHEMA_REGISTRY_URL", errMissing)
	}

	batch, err := strconv.Atoi(envOr("OUTBOX_BATCH", "100"))
	if err != nil {
		return config{}, fmt.Errorf("outbox-relay: OUTBOX_BATCH: %w", err)
	}
	if batch <= 0 {
		return config{}, fmt.Errorf("outbox-relay: OUTBOX_BATCH %d: %w", batch, errMissing)
	}
	settings.batch = batch

	interval, err := time.ParseDuration(envOr("OUTBOX_INTERVAL", "1s"))
	if err != nil {
		return config{}, fmt.Errorf("outbox-relay: OUTBOX_INTERVAL: %w", err)
	}
	if interval <= 0 {
		return config{}, fmt.Errorf("outbox-relay: OUTBOX_INTERVAL %s: %w", interval, outbox.ErrInterval)
	}
	settings.interval = interval

	return settings, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
