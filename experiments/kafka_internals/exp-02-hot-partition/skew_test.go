package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"
)

// The package runs consumer groups in goroutines, so a leak would mean a member that
// outlived the measurement it belongs to. No test here starts one yet, so this currently
// guards the next test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestDistributionPutsTheHotPartitionFirst(t *testing.T) {
	tests := []struct {
		name         string
		perPartition map[int32]int
		want         []share
	}{
		{
			name:         "one partition holds four events in five",
			perPartition: map[int32]int{0: 100, 3: 800, 5: 100},
			want: []share{
				{Partition: 3, Records: 800, Percent: 80},
				{Partition: 0, Records: 100, Percent: 10},
				{Partition: 5, Records: 100, Percent: 10},
			},
		},
		{
			name:         "an even spread still sorts by partition, so runs compare",
			perPartition: map[int32]int{1: 50, 0: 50},
			want: []share{
				{Partition: 0, Records: 50, Percent: 50},
				{Partition: 1, Records: 50, Percent: 50},
			},
		},
		{
			name:         "nothing produced",
			perPartition: map[int32]int{},
			want:         []share{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, distribution(tt.perPartition)); diff != "" {
				t.Errorf("distribution mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestHottestIsTheFloorUnderAnyDrain(t *testing.T) {
	got := hottest(map[int32]int{0: 100, 3: 800, 5: 100})
	want := share{Partition: 3, Records: 800, Percent: 80}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("hottest mismatch (-want +got):\n%s", diff)
	}

	if empty := hottest(map[int32]int{}); empty != (share{}) {
		t.Errorf("hottest of nothing = %+v, want the zero share", empty)
	}
}

func TestIdleCountsMembersThatHandledNothing(t *testing.T) {
	tests := []struct {
		name        string
		perConsumer []int
		want        int
	}{
		{name: "every member did some work", perConsumer: []int{10, 10, 10}, want: 0},
		{name: "a seventh member on six partitions waits", perConsumer: []int{10, 10, 10, 10, 10, 10, 0}, want: 1},
		{name: "one member took the lot", perConsumer: []int{1000, 0, 0}, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := idle(tt.perConsumer); got != tt.want {
				t.Errorf("idle = %d, want %d", got, tt.want)
			}
		})
	}
}

// The ceiling is what makes the skewed cell's result a consequence of its design rather
// than a finding: at 85% on one partition no group can drain more than 1.17× faster.
func TestTheCeilingIsTheTopicOverItsHottestPartition(t *testing.T) {
	tests := []struct {
		name    string
		events  int
		hottest int
		want    float64
	}{
		{name: "the skewed cell: 17 052 of 20 000 on one partition", events: 20_000, hottest: 17_052, want: 20_000.0 / 17_052},
		{name: "six even partitions", events: 600, hottest: 100, want: 6},
		{name: "nothing produced", events: 0, hottest: 0, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, ceiling(tt.events, tt.hottest)); diff != "" {
				t.Errorf("ceiling (-want +got):\n%s", diff)
			}
		})
	}
}
