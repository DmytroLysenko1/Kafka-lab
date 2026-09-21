package labkit

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrDatabase = errors.New("labkit: postgres")

// Postgres builds a pool from a parsed config, so that no error message can carry the
// password out of the connection string and into a log line: every failure names the host,
// port and database, and nothing else.
func Postgres(ctx context.Context, url string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("%w: the database url is not a valid connection string", ErrDatabase)
	}
	where := fmt.Sprintf("%s:%d/%s", config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("%w: connect to %s", ErrDatabase, where)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: ping %s", ErrDatabase, where)
	}
	return pool, nil
}
