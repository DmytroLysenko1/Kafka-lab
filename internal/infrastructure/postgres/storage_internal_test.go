package postgres

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// A failed COMMIT is reported as not applied only on an answer Postgres documents as a
// rollback. Everything else is an unknown outcome, because a payment reported as not kept
// when it was is the one mistake a caller cannot recover from; one reported as unknown is
// settled by retrying with the same idempotency key.
func TestACommitIsReportedAsNotAppliedOnlyOnAnAnswerThatMeansRollback(t *testing.T) {
	type args struct {
		err error
	}
	type want struct {
		NotApplied bool
		Unknown    bool
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{name: "committed", args: args{err: nil}, want: want{}},
		{name: "the ROLLBACK tag for a transaction that had already failed", args: args{err: pgx.ErrTxCommitRollback}, want: want{NotApplied: true}},
		{name: "a serialization failure", args: args{err: &pgconn.PgError{SeverityUnlocalized: "ERROR", Code: "40001"}}, want: want{NotApplied: true}},
		{name: "a deferred constraint", args: args{err: &pgconn.PgError{SeverityUnlocalized: "ERROR", Code: "23505"}}, want: want{NotApplied: true}},
		{name: "a deadlock", args: args{err: &pgconn.PgError{SeverityUnlocalized: "ERROR", Code: "40P01"}}, want: want{NotApplied: true}},
		{name: "the server does not know whether the statement completed", args: args{err: &pgconn.PgError{SeverityUnlocalized: "ERROR", Code: "40003"}}, want: want{Unknown: true}},
		{name: "a translated severity, read unlocalized", args: args{err: &pgconn.PgError{Severity: "ОШИБКА", SeverityUnlocalized: "ERROR", Code: "40001"}}, want: want{NotApplied: true}},
		{name: "the server shutting the session down", args: args{err: &pgconn.PgError{SeverityUnlocalized: "FATAL", Code: "57P01"}}, want: want{Unknown: true}},
		{name: "a connection failure a proxy reports", args: args{err: &pgconn.PgError{SeverityUnlocalized: "ERROR", Code: "08006"}}, want: want{Unknown: true}},
		{name: "no answer in time", args: args{err: context.DeadlineExceeded}, want: want{Unknown: true}},
		{name: "the connection gone mid-commit", args: args{err: io.ErrUnexpectedEOF}, want: want{Unknown: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted := commitFailure(tt.args.err)

			got := want{NotApplied: errors.Is(sorted, ErrNotApplied), Unknown: errors.Is(sorted, ErrCommitUnknown)}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("commit failure %v (-want +got):\n%s", tt.args.err, diff)
			}
			if tt.args.err != nil && !errors.Is(sorted, tt.args.err) {
				t.Errorf("the driver's error was dropped from %v", sorted)
			}
		})
	}
}
