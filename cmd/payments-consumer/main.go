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

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
)

var errMissing = errors.New("payments-consumer: required configuration is missing")

type config struct {
	databaseURL string
	brokers     []string
	topic       string
	dlqTopic    string
	group       string
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

	dead, err := kafka.NewDeadLetters(settings.brokers, settings.dlqTopic)
	if err != nil {
		return err
	}
	defer dead.Close()

	consumer, err := kafka.NewConsumer(settings.brokers, settings.topic, settings.group, record, dead, logger)
	if err != nil {
		return err
	}
	defer consumer.Close()

	logger.InfoContext(ctx, "payments consumer started",
		"topic", settings.topic,
		"group", settings.group,
		"dead_letter_topic", settings.dlqTopic,
	)
	return consumer.Run(ctx)
}

func load() (config, error) {
	settings := config{
		databaseURL: os.Getenv("DATABASE_URL"),
		topic:       envOr("CONSUMER_TOPIC", "payments.main"),
		dlqTopic:    envOr("CONSUMER_DLQ_TOPIC", "payments-consumer.dlq"),
		group:       envOr("CONSUMER_GROUP", "payments-consumer"),
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
