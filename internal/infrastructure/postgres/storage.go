package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrConnect       = errors.New("postgres: connect")
	ErrCommitUnknown = errors.New("postgres: commit outcome unknown")
	ErrRollback      = errors.New("postgres: rollback failed")
)

const (
	commitTimeout   = 5 * time.Second
	rollbackTimeout = 5 * time.Second
)

type queryer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Storage struct {
	pool *pgxpool.Pool
}

// New parses the url before connecting so that no failure message can carry the password
// out of the connection string into a log line: a failure names host, port and database.
func New(ctx context.Context, url string) (*Storage, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("%w: the database url is not a valid connection string", ErrConnect)
	}
	where := fmt.Sprintf("%s:%d/%s", config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrConnect, where)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: ping %s", ErrConnect, where)
	}
	return &Storage{
		pool: pool,
	}, nil
}

func (s *Storage) Close() {
	s.pool.Close()
}

type txKey struct{}

func (s *Storage) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if ambient, nested := transaction(ctx); nested {
		savepoint, err := ambient.Begin(ctx)
		if err != nil {
			return fmt.Errorf("postgres.WithinTx savepoint: %w", err)
		}
		return runTx(ctx, savepoint, fn, release)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres.WithinTx begin: %w", err)
	}
	return runTx(ctx, tx, fn, commit)
}

func (s *Storage) conn(ctx context.Context) queryer {
	if tx, inside := transaction(ctx); inside {
		return tx
	}
	return s.pool
}

func transaction(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}

func runTx(ctx context.Context, tx pgx.Tx, fn func(ctx context.Context) error, complete func(context.Context, pgx.Tx) error) (err error) {
	settled := false
	defer func() {
		if settled {
			return
		}
		if rollbackErr := rollback(ctx, tx); rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}
	}()

	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}
	if err := complete(ctx, tx); err != nil {
		return err
	}
	settled = true
	return nil
}

// commit runs on a context that cannot be cancelled: a caller who walks away mid-commit
// would otherwise leave the outcome genuinely unknown while the code reported a failure.
func commit(ctx context.Context, tx pgx.Tx) error {
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()

	err := tx.Commit(commitCtx)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("postgres.commit: %w", errors.Join(ErrCommitUnknown, err))
	default:
		return fmt.Errorf("postgres.commit: %w", err)
	}
}

func release(ctx context.Context, savepoint pgx.Tx) error {
	if err := savepoint.Commit(ctx); err != nil {
		return fmt.Errorf("postgres.release savepoint: %w", err)
	}
	return nil
}

func rollback(ctx context.Context, tx pgx.Tx) error {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
	defer cancel()

	if err := tx.Rollback(rollbackCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("postgres.rollback: %w", errors.Join(ErrRollback, err))
	}
	return nil
}
