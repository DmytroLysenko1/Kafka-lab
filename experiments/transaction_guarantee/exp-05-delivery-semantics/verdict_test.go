package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTallyNamesWhatTheRunDidToThePayments(t *testing.T) {
	type want struct {
		Lost       int
		Duplicated int
		Outcome    outcome
	}

	tests := []struct {
		name string
		args tally
		want want
	}{
		{
			name: "every payment handled once: the inbox run's whole point",
			args: tally{Produced: 1000, Rows: 1000, Distinct: 1000},
			want: want{Outcome: outcomeExactly},
		},
		{
			name: "committed before handling, killed: the uncommitted window is gone for good",
			args: tally{Produced: 1000, Rows: 975, Distinct: 975},
			want: want{Lost: 25, Outcome: outcomeLost},
		},
		{
			name: "committed after handling, killed: the replayed window is charged twice",
			args: tally{Produced: 1000, Rows: 1025, Distinct: 1000},
			want: want{Duplicated: 25, Outcome: outcomeDuplicate},
		},
		{
			// The case that makes Lost a count of missing payments rather than a
			// subtraction of totals: 30 lost and 30 duplicated would net to 1000 rows
			// and read as a clean run.
			name: "lost and duplicated in equal measure: the totals agree and the run is still broken",
			args: tally{Produced: 1000, Rows: 1000, Distinct: 970},
			want: want{Lost: 30, Duplicated: 30, Outcome: outcomeBoth},
		},
		{
			name: "the consumer never ran, which is not the same as losing everything",
			args: tally{Produced: 1000, Rows: 0, Distinct: 0},
			want: want{Lost: 1000, Outcome: outcomeUnread},
		},
		{
			name: "one payment, handled once",
			args: tally{Produced: 1, Rows: 1, Distinct: 1},
			want: want{Outcome: outcomeExactly},
		},
		{
			name: "more distinct payments in the table than were produced cannot go negative",
			args: tally{Produced: 10, Rows: 12, Distinct: 12},
			want: want{Duplicated: 0, Outcome: outcomeExactly},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := want{Lost: tt.args.Lost(), Duplicated: tt.args.Duplicated(), Outcome: tt.args.outcome()}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEveryModeDeclaresWhatItMustDemonstrate(t *testing.T) {
	for _, mode := range modes {
		if _, ok := expected[mode]; !ok {
			t.Errorf("mode %q has no expected outcome, so its run could not fail", mode)
		}
	}
}

func TestShouldDieLandsWhereTheCrashCostsSomething(t *testing.T) {
	type args struct {
		mode        string
		dieAfter    int
		handled     int
		lastOfBatch bool
	}

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "not there yet",
			args: args{mode: modeAtMostOnce, dieAfter: 500, handled: 499},
			want: false,
		},
		{
			// The defect the first live run exposed: the batch was committed before it was
			// written, so dying on its last record leaves nothing committed-but-unwritten.
			name: "at-most-once on a batch boundary would lose nothing, so it waits",
			args: args{mode: modeAtMostOnce, dieAfter: 500, handled: 500, lastOfBatch: true},
			want: false,
		},
		{
			name: "at-most-once mid-batch leaves the rest of a committed batch unwritten",
			args: args{mode: modeAtMostOnce, dieAfter: 500, handled: 500, lastOfBatch: false},
			want: true,
		},
		{
			name: "at-least-once dies before the commit, so a boundary is exactly where it bites",
			args: args{mode: modeAtLeastOnce, dieAfter: 500, handled: 500, lastOfBatch: true},
			want: true,
		},
		{
			name: "inbox dies wherever it likes and must still come out clean",
			args: args{mode: modeInbox, dieAfter: 500, handled: 500, lastOfBatch: true},
			want: true,
		},
		{
			name: "a run that was never asked to die never does",
			args: args{mode: modeAtMostOnce, dieAfter: 0, handled: 9999},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := &consumer{
				cfg:     &settings{mode: tt.args.mode, dieAfter: tt.args.dieAfter},
				handled: tt.args.handled,
			}
			if got := run.shouldDie(tt.args.lastOfBatch); got != tt.want {
				t.Errorf("shouldDie(%v) = %v, want %v", tt.args.lastOfBatch, got, tt.want)
			}
		})
	}
}
