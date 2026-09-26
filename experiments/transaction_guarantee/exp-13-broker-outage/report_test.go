package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func backlogAt(at time.Duration, backlog int) sample {
	return sample{at: at, backlog: backlog, backlogKnown: true, clusterKnown: true}
}

func unreadAt(at time.Duration) sample {
	return sample{at: at}
}

func TestCatchUpIsTimedFromTheRevivalToTheFirstReadingBackAtTheSteadyBacklog(t *testing.T) {
	const revived = 40 * time.Second

	type want struct {
		caught time.Duration
		ok     bool
	}
	tests := []struct {
		name    string
		samples []sample
		steady  int
		want    want
	}{
		{
			name:    "the first reading at or below the steady level ends the catch-up",
			samples: []sample{backlogAt(41*time.Second, 300), backlogAt(55*time.Second, 4), backlogAt(60*time.Second, 2)},
			steady:  4,
			want:    want{caught: 15 * time.Second, ok: true},
		},
		{
			name:    "a low reading taken before the brokers came back does not count",
			samples: []sample{backlogAt(30*time.Second, 1), backlogAt(50*time.Second, 200)},
			steady:  4,
			want:    want{},
		},
		{
			name:    "an unread backlog is not a backlog of zero",
			samples: []sample{unreadAt(45 * time.Second), backlogAt(52*time.Second, 3)},
			steady:  4,
			want:    want{caught: 12 * time.Second, ok: true},
		},
		{
			name:    "a backlog that never came down is reported as never caught up",
			samples: []sample{backlogAt(45*time.Second, 90), backlogAt(80*time.Second, 50)},
			steady:  4,
			want:    want{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caught, ok := caughtUp(tt.samples, revived, tt.steady)
			if diff := cmp.Diff(tt.want, want{caught: caught, ok: ok}, cmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("caughtUp mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheWorstOfAPhaseIsTakenOnlyFromReadingsInsideIt(t *testing.T) {
	span := phase{name: "brokers down", from: 20 * time.Second, until: 40 * time.Second}
	samples := []sample{
		backlogAt(10*time.Second, 900),
		{at: 25 * time.Second, backlog: 120, backlogKnown: true, underReplicated: 3, clusterKnown: true},
		{at: 30 * time.Second, backlog: 300, backlogKnown: true},
		unreadAt(35 * time.Second),
		backlogAt(40*time.Second, 800),
	}

	got := worst(samples, span)

	want := sample{backlog: 300, backlogKnown: true, underReplicated: 3, clusterKnown: true}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(sample{})); diff != "" {
		t.Errorf("worst mismatch (-want +got):\n%s", diff)
	}
}

func TestAReportWithoutReadingsSaysUnknownWhereItWouldOtherwisePrintZero(t *testing.T) {
	cfg := &settings{victims: "kafka1,kafka2", window: 60 * time.Second}
	tests := []struct {
		name    string
		samples []sample
		want    []string
	}{
		{
			name:    "no backlog read while the brokers were down",
			samples: []sample{backlogAt(5*time.Second, 2), unreadAt(25 * time.Second), backlogAt(50*time.Second, 2)},
			want:    []string{"brokers kafka1,kafka2 down", "unknown"},
		},
		{
			name:    "no backlog read before the kill",
			samples: []sample{unreadAt(5 * time.Second), backlogAt(50*time.Second, 2)},
			want:    []string{"no backlog was read before the kill"},
		},
		{
			name:    "no backlog read at all",
			samples: []sample{unreadAt(5 * time.Second)},
			want:    []string{"peak backlog: unknown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := report(&out, cfg, tt.samples, nil, 20*time.Second, 40*time.Second, time.Second); err != nil {
				t.Fatalf("report: %v", err)
			}
			if !containsLineWith(out.String(), tt.want...) {
				t.Errorf("no line holds %q in:\n%s", tt.want, out.String())
			}
		})
	}
}

func containsLineWith(text string, parts ...string) bool {
	for line := range strings.Lines(text) {
		if all(line, parts) {
			return true
		}
	}
	return false
}

func all(line string, parts []string) bool {
	for _, part := range parts {
		if !strings.Contains(line, part) {
			return false
		}
	}
	return true
}
