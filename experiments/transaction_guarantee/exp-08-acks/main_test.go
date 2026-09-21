package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/goleak"
)

// The package builds kgo clients, whose goroutines are reaped only if Close runs to
// completion. No test here drives one yet, so this currently guards the next test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRefusalClaimsOnlyTheFailureThisHalfExistsToShow(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "every write accepted", err: nil, want: "no"},
		{name: "the in-sync set is short of min.insync.replicas", err: kerr.NotEnoughReplicas, want: "yes, NOT_ENOUGH_REPLICAS"},
		{
			name: "appended, then the in-sync set shrank before the acknowledgement: still a refusal",
			err:  kerr.NotEnoughReplicasAfterAppend,
			want: "yes, NOT_ENOUGH_REPLICAS",
		},
		{
			name: "the refusal arrives wrapped, the way the client surfaces it",
			err:  fmt.Errorf("produce: %w", kerr.NotEnoughReplicas),
			want: "yes, NOT_ENOUGH_REPLICAS",
		},
		{
			// The case that makes this a function worth testing: a deadline is not the
			// setting working, and reading it as one would publish a refusal that never
			// happened.
			name: "the run timed out instead",
			err:  context.DeadlineExceeded,
			want: "no — failed for another reason: context deadline exceeded",
		},
		{
			name: "no leader to ask",
			err:  kerr.LeaderNotAvailable,
			want: "no — failed for another reason: " + kerr.LeaderNotAvailable.Error(),
		},
		{
			name: "some unrelated error",
			err:  errors.New("broken pipe"),
			want: "no — failed for another reason: broken pipe",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := refusal(tt.err); got != tt.want {
				t.Errorf("refusal(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}
