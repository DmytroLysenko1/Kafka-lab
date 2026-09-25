package main

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/env"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/lifecycle"
)

type config struct {
	databaseURL string
	brokers     []string
	topic       string
	dlqTopic    string
	group       string
	metricsAddr string
}

func main() { lifecycle.Main("payments consumer", run) }

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
	for _, tier := range tiers {
		chain = append(chain, kafka.Stage{Topic: tier.topic, Group: tier.topic, Delay: tier.delay})
	}
	return kafka.Chain(chain...)
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
	var read env.Reader
	settings := config{
		databaseURL: read.Required("DATABASE_URL"),
		brokers:     read.List("KAFKA_BROKERS"),
		topic:       env.Or("CONSUMER_TOPIC", "payments.main"),
		dlqTopic:    env.Or("CONSUMER_DLQ_TOPIC", "payments-consumer.dlq"),
		group:       env.Or("CONSUMER_GROUP", "payments-consumer"),
		metricsAddr: env.Or("METRICS_ADDR", "127.0.0.1:9102"),
	}
	return settings, read.Err()
}
