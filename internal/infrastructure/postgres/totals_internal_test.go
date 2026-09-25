package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
)

// Only a lock wait that ran out is contention: it sends the record into the retry chain
// and lets the partition move. Anything else read as contention would feed the chain with
// failures that are not this record's — a database that is down would drain the whole
// topic into the dead letter topic in eleven minutes instead of holding it.
func TestOnlyARunOutLockWaitIsReportedAsContention(t *testing.T) {
	type args struct {
		err error
	}
	type want struct {
		contended bool
		storage   bool
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "lock_timeout expired, wrapped by the driver",
			args: args{err: fmt.Errorf("exec: %w", &pgconn.PgError{Code: "55P03"})},
			want: want{contended: true},
		},
		{
			name: "the CHECK on the total refused it",
			args: args{err: &pgconn.PgError{Code: "23514"}},
			want: want{storage: true},
		},
		{
			name: "the connection went away",
			args: args{err: errors.New("unexpected EOF")},
			want: want{storage: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translated := translateAddFailure(tt.args.err)
			got := want{
				contended: errors.Is(translated, merchants.ErrContended),
				storage:   errors.Is(translated, ErrTotals),
			}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("translated %v (-want +got):\n%s", tt.args.err, diff)
			}
			if !errors.Is(translated, tt.args.err) {
				t.Errorf("the driver's error was dropped from %v", translated)
			}
		})
	}
}
