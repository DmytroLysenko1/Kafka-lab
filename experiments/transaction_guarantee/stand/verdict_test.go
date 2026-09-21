package stand

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTallyNamesWhatTheRunDidToThePayments(t *testing.T) {
	type want struct {
		Lost       int
		Duplicated int
		Outcome    Outcome
	}

	tests := []struct {
		name string
		args Tally
		want want
	}{
		{
			name: "every payment handled once: the inbox run's whole point",
			args: Tally{Produced: 1000, Rows: 1000, Distinct: 1000},
			want: want{Outcome: OutcomeExactly},
		},
		{
			name: "committed before handling, killed: the uncommitted window is gone for good",
			args: Tally{Produced: 1000, Rows: 975, Distinct: 975},
			want: want{Lost: 25, Outcome: OutcomeLost},
		},
		{
			name: "committed after handling, killed: the replayed window is charged twice",
			args: Tally{Produced: 1000, Rows: 1025, Distinct: 1000},
			want: want{Duplicated: 25, Outcome: OutcomeDuplicate},
		},
		{
			// The case that makes Lost a count of missing payments rather than a
			// subtraction of totals: 30 lost and 30 duplicated would net to 1000 rows
			// and read as a clean run.
			name: "lost and duplicated in equal measure: the totals agree and the run is still broken",
			args: Tally{Produced: 1000, Rows: 1000, Distinct: 970},
			want: want{Lost: 30, Duplicated: 30, Outcome: OutcomeBoth},
		},
		{
			name: "the consumer never ran, which is not the same as losing everything",
			args: Tally{Produced: 1000, Rows: 0, Distinct: 0},
			want: want{Lost: 1000, Outcome: OutcomeUnread},
		},
		{
			name: "one payment, handled once",
			args: Tally{Produced: 1, Rows: 1, Distinct: 1},
			want: want{Outcome: OutcomeExactly},
		},
		{
			name: "more distinct payments in the table than were produced cannot go negative",
			args: Tally{Produced: 10, Rows: 12, Distinct: 12},
			want: want{Duplicated: 0, Outcome: OutcomeExactly},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := want{Lost: tt.args.Lost(), Duplicated: tt.args.Duplicated(), Outcome: tt.args.Outcome()}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEveryModeDeclaresWhatItMustDemonstrate(t *testing.T) {
	for _, mode := range []Mode{AtMostOnce, AtLeastOnce, Inbox} {
		if mode.Expects() == "" {
			t.Errorf("mode %q has no expected outcome, so its run could not fail", mode)
		}
	}
}

func TestVerdictRefusesARunThatDidNotExerciseItsMode(t *testing.T) {
	type args struct {
		mode  Mode
		tally Tally
	}

	const asExpected = "as expected"
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "the inbox refused the replayed batch and the table is clean: deduplication shown",
			args: args{mode: Inbox, tally: Tally{Produced: 1000, Rows: 1000, Distinct: 1000, Refused: 50}},
			want: asExpected,
		},
		{
			// The gap the audit found: without the refusal count this run printed "exactly
			// once" and could not be told apart from one that was never redelivered anything.
			name: "a clean inbox table with nothing redelivered proves nothing about the inbox",
			args: args{mode: Inbox, tally: Tally{Produced: 1000, Rows: 1000, Distinct: 1000}},
			want: "EXPECTED the inbox to refuse redeliveries — none arrived, so a clean table proves nothing",
		},
		{
			name: "an inbox that still let a duplicate through fails on the outcome first",
			args: args{mode: Inbox, tally: Tally{Produced: 1000, Rows: 1001, Distinct: 1000, Refused: 49}},
			want: `EXPECTED "every payment exactly once" — this run did not demonstrate what it exists to demonstrate`,
		},
		{
			name: "at-least-once needs no refusals: its duplicates are the redelivery",
			args: args{mode: AtLeastOnce, tally: Tally{Produced: 1000, Rows: 1050, Distinct: 1000}},
			want: asExpected,
		},
		{
			name: "at-most-once that lost nothing did not demonstrate the loss",
			args: args{mode: AtMostOnce, tally: Tally{Produced: 1000, Rows: 1000, Distinct: 1000}},
			want: `EXPECTED "payments lost" — this run did not demonstrate what it exists to demonstrate`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, tt.args.mode.Verdict(tt.args.tally)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestShouldDieLandsWhereTheCrashCostsSomething(t *testing.T) {
	type args struct {
		mode        Mode
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
			args: args{mode: AtMostOnce, dieAfter: 500, handled: 499},
			want: false,
		},
		{
			// The defect the first live run exposed: the batch was committed before it was
			// written, so dying on its last record leaves nothing committed-but-unwritten.
			name: "at-most-once on a batch boundary would lose nothing, so it waits",
			args: args{mode: AtMostOnce, dieAfter: 500, handled: 500, lastOfBatch: true},
			want: false,
		},
		{
			name: "at-most-once mid-batch leaves the rest of a committed batch unwritten",
			args: args{mode: AtMostOnce, dieAfter: 500, handled: 500, lastOfBatch: false},
			want: true,
		},
		{
			name: "at-least-once dies before the commit, so a boundary is exactly where it bites",
			args: args{mode: AtLeastOnce, dieAfter: 500, handled: 500, lastOfBatch: true},
			want: true,
		},
		{
			name: "inbox dies wherever it likes and must still come out clean",
			args: args{mode: Inbox, dieAfter: 500, handled: 500, lastOfBatch: true},
			want: true,
		},
		{
			name: "a run that was never asked to die never does",
			args: args{mode: AtMostOnce, dieAfter: 0, handled: 9999},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := &consumer{
				cfg:     &Settings{Experiment: Experiment{Mode: tt.args.mode}, DieAfter: tt.args.dieAfter},
				handled: tt.args.handled,
			}
			if got := run.shouldDie(tt.args.lastOfBatch); got != tt.want {
				t.Errorf("shouldDie(%v) = %v, want %v", tt.args.lastOfBatch, got, tt.want)
			}
		})
	}
}
