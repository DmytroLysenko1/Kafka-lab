package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
)

var errMissing = errors.New("payments-consumer: required configuration is missing")

type config struct {
	databaseURL string
	brokers     []string
	topic       string
	dlqTopic    string
	group       string
	metricsAddr string
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	// os.Exit skips deferred calls, so it stays out of the function that holds them.
	if code := start(logger); code != 0 {
		os.Exit(code)
	}
}

func start(logger *slog.Logger) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger); err != nil {
		logger.ErrorContext(ctx, "payments consumer stopped", "error", err)
		return 1
	}
	logger.InfoContext(ctx, "payments consumer stopped")
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

	record := merchants.NewRecordAuthorized(
		postgres.NewInboxStore(storage),
		postgres.NewTotalsStore(storage),
		storage,
		time.Now,
	)

	observed := metrics.New()
	detours, err := kafka.NewDetours(settings.brokers, settings.dlqTopic, observed)
	if err != nil {
		return err
	}
	defer detours.Close()

	consumers, err := provideConsumers(&settings, record, detours, logger, observed)
	defer closeAll(consumers)
	if err != nil {
		return err
	}

	logger.InfoContext(ctx, "payments consumer started",
		"topic", settings.topic,
		"group", settings.group,
		"stages", len(consumers),
		"dead_letter_topic", settings.dlqTopic,
		"metrics", settings.metricsAddr,
	)

	running, stop := errgroup.WithContext(ctx)
	for _, consumer := range consumers {
		running.Go(func() error { return consumer.Run(stop) })
	}
	running.Go(func() error { return metrics.Serve(stop, settings.metricsAddr, observed.Gatherer()) })
	return running.Wait()
}

// stages is the main topic followed by the retry tiers, each tier a group of its own: a
// tier waiting ten minutes for its head record must not hold a rebalance of the main
// group, and a separate group is what keeps their offsets and members apart. The tier
// topics are the catalog's (deploy/topics/), and each carries its delay in its name so
// that nobody reading a topic list has to guess what it holds records for.
func stages(settings *config) []kafka.Stage {
	tiers := []struct {
		topic string
		delay time.Duration
	}{
		{topic: "payments-consumer.retry.5s", delay: 5 * time.Second},
		{topic: "payments-consumer.retry.1m", delay: time.Minute},
		{topic: "payments-consumer.retry.10m", delay: 10 * time.Minute},
	}

	chain := make([]kafka.Stage, 0, 1+len(tiers))
	chain = append(chain, kafka.Stage{Topic: settings.topic, Group: settings.group})
	for position, tier := range tiers {
		chain[len(chain)-1].Next = tier.topic
		chain = append(chain, kafka.Stage{Topic: tier.topic, Group: tier.topic, Tier: position + 1, Delay: tier.delay})
	}
	return chain
}

// provideConsumers returns every consumer it built, even when a later one failed, so that
// the caller closes the ones that did open.
func provideConsumers(settings *config, record *merchants.RecordAuthorized, detours *kafka.Detours, logger *slog.Logger, observed *metrics.Registry) ([]*kafka.Consumer, error) {
	chain := stages(settings)
	consumers := make([]*kafka.Consumer, 0, len(chain))
	for _, stage := range chain {
		consumer, err := kafka.NewConsumer(settings.brokers, stage, record, detours, logger, observed)
		if err != nil {
			return consumers, err
		}
		consumers = append(consumers, consumer)
	}
	return consumers, nil
}

func closeAll(consumers []*kafka.Consumer) {
	for _, consumer := range consumers {
		consumer.Close()
	}
}

func load() (config, error) {
	settings := config{
		databaseURL: os.Getenv("DATABASE_URL"),
		topic:       envOr("CONSUMER_TOPIC", "payments.main"),
		dlqTopic:    envOr("CONSUMER_DLQ_TOPIC", "payments-consumer.dlq"),
		group:       envOr("CONSUMER_GROUP", "payments-consumer"),
		metricsAddr: envOr("METRICS_ADDR", "127.0.0.1:9102"),
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
	}
	return settings, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
