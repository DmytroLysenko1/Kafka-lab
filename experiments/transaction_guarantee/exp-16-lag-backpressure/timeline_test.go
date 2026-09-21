package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCountChecksEveryAcknowledgedRecord(t *testing.T) {
	type args struct {
		seen     map[int64]int
		produced int64
	}

	tests := []struct {
		name string
		args args
		want tally
	}{
		{
			name: "every record handled once",
			args: args{seen: map[int64]int{0: 1, 1: 1, 2: 1}, produced: 3},
			want: tally{},
		},
		{
			// A record handled twice and one never handled must not net off: the totals
			// would agree and the run would be broken twice over.
			name: "one lost and one doubled are both reported",
			args: args{seen: map[int64]int{0: 2, 2: 1}, produced: 3},
			want: tally{missing: 1, twice: 1},
		},
		{
			name: "records handled beyond what was acknowledged are not charged as anything",
			args: args{seen: map[int64]int{0: 1, 1: 1, 7: 1}, produced: 2},
			want: tally{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, count(tt.args.seen, tt.args.produced), cmp.AllowUnexported(tally{})); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDrainedAtIsTheFirstLowSampleAfterTheOutage(t *testing.T) {
	s := func(sec int, lag int64) lagSample { return lagSample{at: time.Duration(sec) * time.Second, lag: lag} }
	lags := []lagSample{s(10, 5), s(20, 4000), s(35, 6000), s(38, 900), s(41, 60), s(44, 20)}

	tests := []struct {
		name   string
		after  time.Duration
		want   time.Duration
		wantOK bool
	}{
		{name: "a low sample before the outage ended does not count", after: 35 * time.Second, want: 41 * time.Second, wantOK: true},
		{name: "a lag that never comes down is reported as such", after: 50 * time.Second, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := drainedAt(lags, tt.after, drainedBelow)
			if diff := cmp.Diff(tt.want, got); diff != "" || ok != tt.wantOK {
				t.Errorf("drainedAt() = %s, %v, want %s, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestRewindToIsTheFirstUnhandledRecordOfEachPartition(t *testing.T) {
	rec := func(partition int32, offset int64) *kgo.Record {
		return &kgo.Record{Topic: topic, Partition: partition, Offset: offset}
	}
	unhandled := []*kgo.Record{rec(1, 40), rec(1, 41), rec(0, 17), rec(2, 9), rec(0, 18)}

	want := map[string]map[int32]kgo.EpochOffset{topic: {
		0: {Epoch: -1, Offset: 17},
		1: {Epoch: -1, Offset: 40},
		2: {Epoch: -1, Offset: 9},
	}}
	if diff := cmp.Diff(want, rewindTo(unhandled)); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestCommitAcceptedRecordsAnOffsetMovingBackwards(t *testing.T) {
	line := newTimeline(time.Now())
	line.commitAccepted("B", map[int32]int64{0: 500, 1: 480})
	// A stale commit from a member that was removed and came back: partition 0 behind what
	// B stored, partition 1 ahead of it.
	line.commitAccepted("A", map[int32]int64{0: 120, 1: 490})
	// A goes on from where it rewound to: climbing back is not another rewind.
	line.commitAccepted("A", map[int32]int64{0: 220})
	line.commitAccepted("B", map[int32]int64{0: 520})

	got := line.snapshot().rewinds
	want := []rewind{{member: "A", partition: 0, by: 380}}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(rewind{}), cmpopts.IgnoreFields(rewind{}, "at")); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}
