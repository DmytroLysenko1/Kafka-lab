package main

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	payhttp "github.com/DmytroLysenko1/Kafka-lab/internal/interfaces/http"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/env"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/lifecycle"
)

type config struct {
	databaseURL string
	address     string
	apiKey      string
	metricsAddr string
}

func main() { lifecycle.Main("payments api", run) }

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
	var read env.Reader
	settings := config{
		databaseURL: read.Required("DATABASE_URL"),
		// Loopback by default: this service has one shared key and no transport security,
		// which is enough for a lab and not enough for a network anyone else is on.
		address:     env.Or("PAYMENTS_API_ADDR", "127.0.0.1:8081"),
		apiKey:      read.Required("PAYMENTS_API_KEY"),
		metricsAddr: env.Or("METRICS_ADDR", "127.0.0.1:9103"),
	}
	return settings, read.Err()
}
