package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestReachedSeparatesTheBoundFromTheWatching(t *testing.T) {
	kill := time.Date(2026, 9, 20, 20, 7, 22, 0, time.UTC)

	type args struct {
		since  time.Time
		now    time.Time
		waited time.Duration
	}

	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "the cluster changed while the process was watching: both numbers agree",
			args: args{since: kill, now: kill.Add(10600 * time.Millisecond), waited: 10600 * time.Millisecond},
			want: []string{
				"noticed within\t10.6s of the kill",
				"  of that, spent polling\t10.6s",
			},
		},
		{
			name: "already there at the first look: the bound is startup, and the polling time says so",
			args: args{since: kill, now: kill.Add(220 * time.Millisecond), waited: 0},
			want: []string{
				"noticed within\t200ms of the kill",
				"  of that, spent polling\t0s",
			},
		},
		{
			name: "the process started late: the bound exceeds the watching, which is the whole point of printing both",
			args: args{since: kill, now: kill.Add(6 * time.Second), waited: 2 * time.Second},
			want: []string{
				"noticed within\t6s of the kill",
				"  of that, spent polling\t2s",
			},
		},
		{
			name: "no event timestamp from the shell: the bound falls back to what was polled",
			args: args{since: time.Time{}, now: kill.Add(time.Hour), waited: 3500 * time.Millisecond},
			want: []string{
				"noticed within\t3.5s of the kill",
				"  of that, spent polling\t3.5s",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &settings{}
			if !tt.args.since.IsZero() {
				cfg.since = tt.args.since.UnixMilli()
			}

			got := reached(cfg, tt.args.now, "noticed", "the kill", tt.args.waited)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The shell's timestamp and this process's clock are two wall clocks. If they disagree —
// an NTP step, a container clock — the bound must not come out shorter than the polling it
// is supposed to contain, and must never be negative: it falls back to the interval this
// process timed itself.
func TestSinceFallsBackWhenTheTwoClocksDisagree(t *testing.T) {
	kill := time.Date(2026, 9, 20, 20, 7, 22, 0, time.UTC)
	cfg := &settings{since: kill.UnixMilli()}

	tests := []struct {
		name   string
		now    time.Time
		waited time.Duration
		want   time.Duration
	}{
		{
			name:   "the clock stepped backwards past the event",
			now:    kill.Add(-5 * time.Second),
			waited: 2 * time.Second,
			want:   2 * time.Second,
		},
		{
			name:   "the bound would be shorter than the polling it contains",
			now:    kill.Add(time.Second),
			waited: 4 * time.Second,
			want:   4 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := since(cfg, tt.now, tt.waited); got != tt.want {
				t.Errorf("since() = %v, want %v", got, tt.want)
			}
		})
	}
}
