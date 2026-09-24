package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
)

var errNoDatabaseURL = errors.New("migrate: DATABASE_URL is empty")

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
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errNoDatabaseURL
	}

	storage, err := postgres.New(ctx, url)
	if err != nil {
		return err
	}
	defer storage.Close()

	return storage.Migrate(ctx)
}
