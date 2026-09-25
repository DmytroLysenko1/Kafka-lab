package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"
)

var errBroken = errors.New("the database is gone")

// serve registers for signals through signal.NotifyContext, which runs a goroutine until
// its stop is called; a return that skipped the deferred stop would leak it on every run.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// The exit code is what an orchestrator reads: a service that failed and exited 0 is never
// restarted, and one that stopped on purpose and exited 1 is restarted into a loop.
func TestAServiceThatFailedExitsNonZeroAndOneThatStoppedExitsZero(t *testing.T) {
	type args struct {
		result error
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr error
	}{
		{name: "stopped on purpose", args: args{result: nil}, want: 0},
		{name: "failed", args: args{result: errBroken}, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := serve("test service", func(context.Context, *slog.Logger) error { return tt.args.result })
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("exit code (-want +got):\n%s", diff)
			}
		})
	}
}
