package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
)

// A failed add is sorted by what a retry would do, because that decides where the record
// goes. A lock wait that ran out may clear: the record takes the retry chain. A value of this
// record the database refuses never will — a total past bigint's range, text it cannot hold —
// so the record is refused and dead-lettered, and the partition moves on instead of the
// consumer restarting on it forever. Anything else is the database or the code: a CHECK or a
// malformed value could fail every record alike after a bad migration, and a database that is
// down would drain the whole topic into the dead letter topic in eleven minutes — both stop
// the partition instead, loudly.
func TestAFailedAddIsSortedByWhatARetryWouldDo(t *testing.T) {
	type args struct {
		err error
	}
	type want struct {
		Contended bool
		Refused   bool
		Storage   bool
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "lock_timeout expired, wrapped by the driver",
			args: args{err: fmt.Errorf("exec: %w", &pgconn.PgError{Code: "55P03"})},
			want: want{Contended: true},
		},
		{
			name: "the running total would pass bigint's range",
			args: args{err: &pgconn.PgError{Code: "22003"}},
			want: want{Refused: true},
		},
		{
			name: "a NUL byte in a key the domain let through",
			args: args{err: fmt.Errorf("exec: %w", &pgconn.PgError{Code: "22021"})},
			want: want{Refused: true},
		},
		{
			name: "text the database cannot convert",
			args: args{err: &pgconn.PgError{Code: "22P05"}},
			want: want{Refused: true},
		},
		{
			name: "a CHECK, which a bad migration could fail for every record",
			args: args{err: &pgconn.PgError{Code: "23514"}},
			want: want{Storage: true},
		},
		{
			name: "a malformed value, the query's fault rather than this record's",
			args: args{err: &pgconn.PgError{Code: "22P02"}},
			want: want{Storage: true},
		},
		{
			name: "a unique violation is not the values' fault",
			args: args{err: &pgconn.PgError{Code: "23505"}},
			want: want{Storage: true},
		},
		{
			name: "the connection went away",
			args: args{err: errors.New("unexpected EOF")},
			want: want{Storage: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translated := translateAddFailure(tt.args.err)
			got := want{
				Contended: errors.Is(translated, merchants.ErrContended),
				Refused:   errors.Is(translated, merchants.ErrUnprocessable),
				Storage:   errors.Is(translated, merchant.ErrStorage),
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("translated %v (-want +got):\n%s", tt.args.err, diff)
			}
			if !errors.Is(translated, tt.args.err) {
				t.Errorf("the driver's error was dropped from %v", translated)
			}
		})
	}
}
