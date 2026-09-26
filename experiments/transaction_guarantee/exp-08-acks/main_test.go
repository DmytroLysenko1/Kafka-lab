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

// The third cell's first run printed "lost 2000 of the 2000 that were acknowledged" for a
// producer that was acknowledged nothing: loss was measured against what was sent.
func TestLostCountsOnlyWhatTheProducerWasToldYesTo(t *testing.T) {
	tests := []struct {
		name         string
		acknowledged int
		readable     int
		want         string
	}{
		{name: "nothing acknowledged, nothing readable: nothing lost", acknowledged: 0, readable: 0, want: "lost\t0 of the 0 that were acknowledged"},
		{name: "all acknowledged, none readable: all lost", acknowledged: 2000, readable: 0, want: "lost\t2000 of the 2000 that were acknowledged"},
		{name: "all acknowledged, all readable", acknowledged: 2000, readable: 2000, want: "lost\t0 of the 2000 that were acknowledged"},
		{
			// A readable record may be one of the unacknowledged ones, so the count
			// can only bound the loss from below.
			name: "some acknowledged: the shortfall is a floor", acknowledged: 1500, readable: 1200,
			want: "lost\tat least 300 of the 1500 that were acknowledged",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &settings{records: 2000, acknowledged: tt.acknowledged}
			if got := lost(cfg, tt.readable); got != tt.want {
				t.Errorf("lost(%d acknowledged, %d readable) = %q, want %q", tt.acknowledged, tt.readable, got, tt.want)
			}
		})
	}
}
