package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/env"
)

const migrateConns = 2

// Migrations are applied by a command of their own, not by the services on start: two
// replicas coming up together would otherwise race each other through the same schema
// change, and a service that migrates on boot cannot be rolled out before its database.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()

	if err := migrate(ctx); err != nil {
		logger.ErrorContext(ctx, "migrate failed", "error", err)
		os.Exit(1)
	}
	logger.InfoContext(ctx, "schema is up to date")
}

func migrate(ctx context.Context) error {
	var read env.Reader
	url := read.Required("DATABASE_URL")
	if err := read.Err(); err != nil {
		return err
	}

	// Migrations run statement by statement on one connection; the second is headroom, and
	// two is what the connection budget in deploy/helm/payments/values.yaml counts for this.
	storage, err := postgres.New(ctx, url, postgres.MaxConns(migrateConns))
	if err != nil {
		return err
	}
	defer storage.Close()

	return storage.Migrate(ctx)
}
