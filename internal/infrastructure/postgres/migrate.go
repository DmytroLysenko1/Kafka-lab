package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrations embed.FS

var ErrMigrate = errors.New("postgres: migrate")

// Migrate applies the embedded schema. It is called from the composition root, never from a
// store: a process that migrates on its own the moment it touches the database would race
// every other replica rolling out beside it.
func (s *Storage) Migrate(ctx context.Context) (err error) {
	goose.SetBaseFS(migrations)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("%w: dialect: %w", ErrMigrate, err)
	}

	db := stdlib.OpenDBFromPool(s.pool)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close: %w", ErrMigrate, closeErr))
		}
	}()

	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		return fmt.Errorf("%w: %w", ErrMigrate, err)
	}
	return nil
}
