package main

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	paymentuc "github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	payhttp "github.com/DmytroLysenko1/Kafka-lab/internal/interfaces/http"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/env"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/lifecycle"
)

type config struct {
	databaseURL string
	maxConns    int32
	address     string
	apiKey      string
	metricsAddr string
}

func main() { lifecycle.Main("payments-api", run) }

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
	server, err := payhttp.NewServer(provideUseCases(storage), storage, settings.apiKey, logger, observed)
	if err != nil {
		return err
	}

	logger.InfoContext(ctx, "payments api started", "address", settings.address, "metrics", settings.metricsAddr)
	return serve(ctx, &settings, server, observed)
}

// provideUseCases wires every route's use case to Postgres. Every transaction is told in the
// payments' words, so a commit whose answer never arrived reaches a caller as "may be stored".
func provideUseCases(storage *postgres.Storage) payhttp.UseCases {
	payments := postgres.NewPaymentStore(storage)
	transactions := postgres.NewPaymentTransactions(storage)
	return payhttp.UseCases{
		Authorize: paymentuc.NewAuthorizePayment(payments, transactions, time.Now, payment.NewID),
		Capture:   paymentuc.NewCapturePayment(payments, transactions, time.Now),
		Refund:    paymentuc.NewRefundPayment(payments, transactions, time.Now),
		Totals:    merchants.NewReadTotals(postgres.NewTotalsStore(storage)),
	}
}

// serve runs the API and its metrics until either stops: a scrape endpoint that outlived the
// API would answer for a service that no longer takes payments.
func serve(ctx context.Context, settings *config, server *payhttp.Server, observed *metrics.Registry) error {
	running, stop := errgroup.WithContext(ctx)
	running.Go(func() error { return server.Listen(stop, settings.address) })
	running.Go(func() error { return metrics.Serve(stop, settings.metricsAddr, observed.Gatherer()) })
	return running.Wait()
}

func load() (config, error) {
	var read env.Reader
	settings := config{
		databaseURL: read.Required("DATABASE_URL"),
		// Four for requests: a request holds one connection for one short transaction, and
		// 4 × 5 pods (the HPA's max) is 20 of the budget across services, which is in
		// deploy/helm/payments/values.yaml.
		maxConns: read.PositiveInt32("DB_MAX_CONNS", 4),
		// Loopback by default: this service has one shared key and no transport security,
		// which is enough for a lab and not enough for a network anyone else is on.
		address:     env.Or("PAYMENTS_API_ADDR", "127.0.0.1:8081"),
		apiKey:      read.Required("PAYMENTS_API_KEY"),
		metricsAddr: env.Or("METRICS_ADDR", "127.0.0.1:9103"),
	}
	return settings, read.Err()
}
