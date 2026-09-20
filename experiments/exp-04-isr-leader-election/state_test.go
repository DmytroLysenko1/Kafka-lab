package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestInspect(t *testing.T) {
	type args struct {
		partitions []partition
	}
	type want struct {
		state   health
		leaders []int32
	}

	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "every replica in sync and leading its preferred broker",
			args: args{partitions: []partition{
				{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
				{ID: 1, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3, 1}},
			}},
			want: want{
				state:   health{Partitions: 2, Replicas: 3, SmallestISR: 3},
				leaders: []int32{1, 2},
			},
		},
		{
			name: "one broker gone: its partitions lose a replica and one changes hands",
			args: args{partitions: []partition{
				{ID: 0, Leader: 2, Replicas: []int32{1, 2, 3}, ISR: []int32{2, 3}},
				{ID: 1, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3}},
				{ID: 2, Leader: 3, Replicas: []int32{3, 1, 2}, ISR: []int32{3, 2}},
			}},
			want: want{
				state: health{
					Partitions:      3,
					Replicas:        3,
					UnderReplicated: 3,
					OffPreferred:    1,
					SmallestISR:     2,
				},
				leaders: []int32{2, 2, 3},
			},
		},
		{
			name: "a partition with no leader is counted as leaderless, not as off-preferred",
			args: args{partitions: []partition{
				{ID: 0, Leader: -1, Replicas: []int32{1, 2, 3}, ISR: []int32{}},
			}},
			want: want{
				state: health{
					Partitions:      1,
					Replicas:        3,
					UnderReplicated: 1,
					Leaderless:      1,
					SmallestISR:     0,
				},
				leaders: []int32{-1},
			},
		},
		{
			name: "no partitions at all: the smallest ISR stays unset rather than reading zero",
			args: args{partitions: nil},
			want: want{
				state:   health{SmallestISR: -1},
				leaders: []int32{},
			},
		},
		{
			name: "observations arrive out of order, leaders are reported in partition order",
			args: args{partitions: []partition{
				{ID: 2, Leader: 3, Replicas: []int32{3, 1, 2}, ISR: []int32{3, 1, 2}},
				{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
				{ID: 1, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3, 1}},
			}},
			want: want{
				state:   health{Partitions: 3, Replicas: 3, SmallestISR: 3},
				leaders: []int32{1, 2, 3},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := want{state: inspect(tt.args.partitions), leaders: leaders(tt.args.partitions)}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPreferredIsTheFirstReplica(t *testing.T) {
	tests := []struct {
		name string
		args partition
		want int32
	}{
		{
			name: "the first replica is preferred however leadership moved",
			args: partition{Leader: 3, Replicas: []int32{1, 2, 3}, ISR: []int32{2, 3}},
			want: 1,
		},
		{
			name: "a partition with no assignment has no preferred replica",
			args: partition{Leader: -1},
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.args.preferred(); got != tt.want {
				t.Errorf("preferred() = %d, want %d", got, tt.want)
			}
		})
	}
}
