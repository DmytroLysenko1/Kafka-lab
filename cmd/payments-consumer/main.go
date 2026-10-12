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
	maxConns    int32
	brokers     []string
	topic       string
	dlqTopic    string
	group       string
	metricsAddr string
}

func main() { lifecycle.Main("payments-consumer", run) }

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

	observed := metrics.New()
	detours, err := kafka.NewDetours(settings.brokers, settings.dlqTopic, observed)
	if err != nil {
		return err
	}
	defer detours.Close()

	consumers, err := provideConsumers(&settings, provideRecorder(storage), detours, logger, observed)
	defer closeAll(consumers)
	if err != nil {
		return err
	}

	announce(ctx, logger, &settings, len(consumers))
	return serve(ctx, &settings, consumers, observed)
}

func announce(ctx context.Context, logger *slog.Logger, settings *config, stages int) {
	logger.InfoContext(ctx, "payments consumer started",
		"topic", settings.topic,
		"group", settings.group,
		"stages", stages,
		"dead_letter_topic", settings.dlqTopic,
		"metrics", settings.metricsAddr,
	)
}

// provideRecorder counts each event once: the inbox claim and the total in one transaction,
// told in the merchants' words.
func provideRecorder(storage *postgres.Storage) *merchants.RecordMovement {
	return merchants.NewRecordMovement(
		postgres.NewInboxStore(storage),
		postgres.NewTotalsStore(storage),
		postgres.NewMerchantTransactions(storage),
		time.Now,
	)
}

// serve runs every stage and the metrics until any of them stops: one stage failing stops the
// rest, and the orchestrator restarts the whole consumer rather than a part of its chain.
func serve(ctx context.Context, settings *config, consumers []*kafka.Consumer, observed *metrics.Registry) error {
	running, stop := errgroup.WithContext(ctx)
	for _, consumer := range consumers {
		running.Go(func() error { return consumer.Run(stop) })
	}
	running.Go(func() error { return metrics.Serve(stop, settings.metricsAddr, observed.Gatherer()) })
	return running.Wait()
}

// provideConsumers returns every consumer it built, even when a later one failed, so that
// the caller closes the ones that did open.
func provideConsumers(settings *config, record *merchants.RecordMovement, detours *kafka.Detours, logger *slog.Logger, observed *metrics.Registry) ([]*kafka.Consumer, error) {
	chain := kafka.PaymentsChain(settings.topic, settings.group)
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
		// One per stage — the main topic and three retry tiers run side by side, each handling
		// one record at a time in one transaction — plus one spare. The budget across services
		// is in deploy/helm/payments/values.yaml.
		maxConns:    read.PositiveInt32("DB_MAX_CONNS", 5),
		brokers:     read.List("KAFKA_BROKERS"),
		topic:       env.Or("CONSUMER_TOPIC", "payments.main"),
		dlqTopic:    env.Or("CONSUMER_DLQ_TOPIC", "payments-consumer.dlq"),
		group:       env.Or("CONSUMER_GROUP", "payments-consumer"),
		metricsAddr: env.Or("METRICS_ADDR", "127.0.0.1:9102"),
	}
	return settings, read.Err()
}
