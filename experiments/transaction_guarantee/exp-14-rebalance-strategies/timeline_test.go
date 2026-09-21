package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func at(ms int, partition int32, seq int64) handling {
	return handling{at: time.Duration(ms) * time.Millisecond, partition: partition, seq: seq}
}

func TestLongestGapsChargesAStoppedPartitionUpToTheWindowEdge(t *testing.T) {
	type args struct {
		handled  []handling
		from, to time.Duration
	}

	tests := []struct {
		name string
		args args
		want map[int32]time.Duration
	}{
		{
			// A partition that is never handled inside the window stopped for all of it,
			// which is the eager case where nothing resumes for a long time.
			name: "a partition with nothing handled is charged the whole window",
			args: args{from: 0, to: 1000 * time.Millisecond},
			want: map[int32]time.Duration{0: time.Second, 1: time.Second, 2: time.Second, 3: time.Second, 4: time.Second, 5: time.Second},
		},
		{
			name: "the gap in the middle of a flowing partition is what is reported",
			args: args{
				handled: []handling{at(100, 0, 1), at(150, 0, 2), at(900, 0, 3), at(950, 0, 4)},
				from:    0, to: time.Second,
			},
			want: map[int32]time.Duration{0: 750 * time.Millisecond, 1: time.Second, 2: time.Second, 3: time.Second, 4: time.Second, 5: time.Second},
		},
		{
			name: "records outside the window do not count, and order of arrival does not matter",
			args: args{
				handled: []handling{at(1200, 1, 9), at(500, 1, 5), at(100, 1, 1), at(-50, 1, 0)},
				from:    0, to: time.Second,
			},
			want: map[int32]time.Duration{0: time.Second, 1: 500 * time.Millisecond, 2: time.Second, 3: time.Second, 4: time.Second, 5: time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, longestGaps(tt.args.handled, tt.args.from, tt.args.to)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGroupGapIsWhenNoPartitionMoved(t *testing.T) {
	// Partition 0 stops at 200 and partition 1 resumes at 600: the group was silent for 400,
	// even though each partition on its own was silent for longer.
	handled := []handling{at(100, 0, 1), at(200, 0, 2), at(600, 1, 3), at(700, 1, 4), at(1000, 0, 5)}
	if diff := cmp.Diff(400*time.Millisecond, groupGap(handled, 0, time.Second)); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestHandledTwiceCountsRecordsNotDeliveries(t *testing.T) {
	// seq 1 handled three times is one record handled more than once, not two.
	handled := []handling{at(1, 0, 1), at(2, 0, 1), at(3, 0, 1), at(4, 0, 2), at(5, 1, 3), at(6, 1, 3)}
	if diff := cmp.Diff(2, handledTwice(handled)); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestMissedInsideCatchesAGapThatDuplicatesWouldHide(t *testing.T) {
	// Partition 0 carries seqs 0, 6, 12, 18 and partition 1 carries 1, 7, 13. The run
	// handled 12 twice and never handled 7: a commit of records nobody handled leaves
	// exactly this shape, and counting duplicates alone reports the run as clean.
	handled := []handling{
		at(10, 0, 0), at(20, 0, 6), at(30, 0, 12), at(35, 0, 12), at(40, 0, 18),
		at(15, 1, 1), at(45, 1, 13),
	}
	if diff := cmp.Diff(1, missedInside(handled)); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(1, handledTwice(handled)); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestMissedInsideIgnoresWhatFellOutsideTheRun(t *testing.T) {
	// A partition the group only started reading late, and stopped early: nothing between
	// its own first and last is missing, so nothing is charged.
	handled := []handling{at(10, 2, 200), at(20, 2, 206), at(30, 2, 212)}
	if diff := cmp.Diff(0, missedInside(handled)); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}
