package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrConnect       = errors.New("postgres: connect")
	ErrNotApplied    = errors.New("postgres: the transaction was not applied")
	ErrCommitUnknown = errors.New("postgres: commit outcome unknown")
	ErrRollback      = errors.New("postgres: rollback failed")
)

// The commit and the rollback each get a bound of their own on a context that cannot be
// cancelled (see commit). Both are exported for the consumer's time budget
// (kafka/budget_test.go): a handling that succeeded still spends up to CommitTimeout
// committing, one that failed up to RollbackTimeout rolling back — never both.
const (
	CommitTimeout   = 5 * time.Second
	RollbackTimeout = 5 * time.Second
)

// refusedValueCodes are the data exceptions that belong to the one record carrying the value:
// a total pushed past bigint's range (22003), text the database cannot hold (22021, 22P05). An
// allowlist on purpose: the rest of class 22, or a CHECK, can just as well come from a bad
// migration or a bug in a query, which fails every record alike — sending those to the dead
// letter topic would drain the whole partition there quietly, where stopping is loud.
func refusedValueCodes() []string {
	return []string{"22003", "22021", "22P05"}
}

// Answers to a COMMIT that Postgres documents as a rolled-back transaction: a serialization
// failure (40001), a deadlock (40P01), a rollback for an integrity constraint (40002), and a
// deferred constraint (class 23). Listed, not matched by class 40 as a whole: 40003 is
// statement_completion_unknown, the one answer that says the outcome is NOT known. A
// transaction that had already failed answers COMMIT with the ROLLBACK tag, which pgx reports
// as ErrTxCommitRollback. Only an ERROR counts — compared unlocalized, since the server may
// translate it: a FATAL or PANIC ends the session, and a proxy in between may say either after
// the commit reached the server.
const (
	errorSeverity   = "ERROR"
	constraintClass = "23"
)

func rolledBackCodes() []string {
	return []string{"40001", "40P01", "40002"}
}

type queryer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Storage struct {
	pool *pgxpool.Pool
	// probe answers Ping on a connection of its own. Pinging through the working pool would
	// wait behind every request holding a connection, and a pod whose pool is merely busy
	// would read as not ready — on every pod at once, since they are busy together.
	probe *pgxpool.Pool
}

// Option adjusts the pool before it connects.
type Option func(*pgxpool.Config)

// MaxConns caps the working pool. Left unset, pgx sizes it from the CPU count the process
// sees — the node's, in a container — which multiplied by replicas can exceed what Postgres
// accepts (max_connections). Each service sets its share; the budget is in
// deploy/helm/payments/values.yaml.
func MaxConns(n int32) Option {
	return func(config *pgxpool.Config) {
		config.MaxConns = n
	}
}

// New parses the url before connecting so that no failure message can carry the password
// out of the connection string into a log line: a failure names host, port and database.
func New(ctx context.Context, url string, options ...Option) (*Storage, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("%w: the database url is not a valid connection string", ErrConnect)
	}
	where := fmt.Sprintf("%s:%d/%s", config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database)
	// Every statement is a span in the trace of the request or record that ran it: the hop the
	// trace ends at, and the one a slow request is most often waiting on.
	config.ConnConfig.Tracer = otelpgx.NewTracer()
	for _, option := range options {
		option(config)
	}

	pool, err := connect(ctx, config, where)
	if err != nil {
		return nil, err
	}
	probe, err := pgxpool.NewWithConfig(ctx, probeConfig(config))
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: probe %s", ErrConnect, where)
	}
	return &Storage{
		pool:  pool,
		probe: probe,
	}, nil
}

func connect(ctx context.Context, config *pgxpool.Config, where string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrConnect, where)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: ping %s", ErrConnect, where)
	}
	return pool, nil
}

// probeConfig is one connection, opened only when something first asks: services that are
// never probed never hold it.
func probeConfig(config *pgxpool.Config) *pgxpool.Config {
	probe := config.Copy()
	probe.MaxConns = 1
	probe.MinConns = 0
	// The kubelet asks every few seconds; a span per ping would bury the traces worth reading.
	probe.ConnConfig.Tracer = nil
	return probe
}

func (s *Storage) Close() {
	s.probe.Close()
	s.pool.Close()
}

func (s *Storage) Ping(ctx context.Context) error {
	if err := s.probe.Ping(ctx); err != nil {
		return fmt.Errorf("postgres.Ping: %w", errors.Join(ErrConnect, err))
	}
	return nil
}

type txKey struct{}

// WithinTx runs fn in one transaction. Called inside one already, it joins it: the work is
// part of the outer transaction and commits or rolls back with it — no savepoint, so a failure
// inside is the outer transaction's failure, said in its words.
func (s *Storage) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, inside := transaction(ctx); inside {
		return fn(ctx)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres.WithinTx begin: %w", errors.Join(ErrNotApplied, err))
	}
	return runTx(ctx, tx, fn)
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

func runTx(ctx context.Context, tx pgx.Tx, fn func(ctx context.Context) error) (err error) {
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
	if err := commit(ctx, tx); err != nil {
		return err
	}
	settled = true
	return nil
}

// commit runs on a context that cannot be cancelled: a caller who walks away mid-commit
// would otherwise leave the outcome genuinely unknown while the code reported a failure.
func commit(ctx context.Context, tx pgx.Tx) error {
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CommitTimeout)
	defer cancel()

	return commitFailure(tx.Commit(commitCtx))
}

// commitFailure sorts a failed COMMIT by what is known about it. Only an answer Postgres
// documents as a rollback means the transaction was not applied; anything else — a timeout, a
// connection gone mid-commit, a session ended — leaves the outcome with the server, and is
// reported as unknown. Erring that way is safe: a caller who retries with the same idempotency
// key finds out which; one told "not applied" when it was would be told a lie.
func commitFailure(err error) error {
	switch {
	case err == nil:
		return nil
	case rolledBackAtCommit(err):
		return fmt.Errorf("postgres.commit: %w", errors.Join(ErrNotApplied, err))
	}
	return fmt.Errorf("postgres.commit: %w", errors.Join(ErrCommitUnknown, err))
}

func rolledBackAtCommit(err error) bool {
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return true
	}
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.SeverityUnlocalized != errorSeverity {
		return false
	}
	return slices.Contains(rolledBackCodes(), pgErr.Code) || strings.HasPrefix(pgErr.Code, constraintClass)
}

// refusedByData is whether the database refused the values themselves, which no retry
// changes: the record carrying them has to leave the partition, not stop it.
func refusedByData(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && slices.Contains(refusedValueCodes(), pgErr.Code)
}

func rollback(ctx context.Context, tx pgx.Tx) error {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), RollbackTimeout)
	defer cancel()

	if err := tx.Rollback(rollbackCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("postgres.rollback: %w", errors.Join(ErrRollback, err))
	}
	return nil
}
