package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	payhttp "github.com/DmytroLysenko1/Kafka-lab/internal/interfaces/http"
)

var errMissing = errors.New("payments-api: required configuration is missing")

type config struct {
	databaseURL string
	address     string
	apiKey      string
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
		logger.ErrorContext(ctx, "payments api stopped", "error", err)
		return 1
	}
	logger.InfoContext(ctx, "payments api stopped")
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

	authorize := payments.NewAuthorizePayment(
		postgres.NewPaymentStore(storage),
		storage,
		time.Now,
		payment.NewID,
	)
	totals := merchants.NewReadTotal(postgres.NewTotalsStore(storage))

	observed := metrics.New()
	server, err := payhttp.NewServer(authorize, totals, settings.apiKey, logger, observed)
	if err != nil {
		return err
	}

	logger.InfoContext(ctx, "payments api started", "address", settings.address, "metrics", settings.metricsAddr)

	running, stop := errgroup.WithContext(ctx)
	running.Go(func() error { return server.Listen(stop, settings.address) })
	running.Go(func() error { return metrics.Serve(stop, settings.metricsAddr, observed.Gatherer()) })
	return running.Wait()
}

func load() (config, error) {
	settings := config{
		databaseURL: os.Getenv("DATABASE_URL"),
		// Loopback by default: this service has one shared key and no transport security,
		// which is enough for a lab and not enough for a network anyone else is on.
		address:     envOr("PAYMENTS_API_ADDR", "127.0.0.1:8081"),
		apiKey:      os.Getenv("PAYMENTS_API_KEY"),
		metricsAddr: envOr("METRICS_ADDR", "127.0.0.1:9103"),
	}

	switch {
	case settings.databaseURL == "":
		return config{}, fmt.Errorf("%w: DATABASE_URL", errMissing)
	case settings.apiKey == "":
		return config{}, fmt.Errorf("%w: PAYMENTS_API_KEY", errMissing)
	}
	return settings, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
