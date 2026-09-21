package stand

import "testing"

func TestProgressComparesOffsetsRatherThanWaitingForSilence(t *testing.T) {
	type read struct {
		partition int32
		offset    int64
	}

	tests := []struct {
		name          string
		end           map[int32]int64
		committed     map[int32]int64
		reads         []read
		wantReached   bool
		wantRemaining int64
	}{
		{
			name:          "nothing read yet",
			end:           map[int32]int64{0: 3, 1: 3},
			wantRemaining: 6,
		},
		{
			name:          "one partition drained, the other not: the run is not finished",
			end:           map[int32]int64{0: 2, 1: 2},
			reads:         []read{{0, 0}, {0, 1}},
			wantRemaining: 2,
		},
		{
			name:        "every partition read to its last offset",
			end:         map[int32]int64{0: 2, 1: 1},
			reads:       []read{{0, 0}, {0, 1}, {1, 0}},
			wantReached: true,
		},
		{
			name:        "an empty partition is already drained and must not hold the run open",
			end:         map[int32]int64{0: 1, 1: 0},
			reads:       []read{{0, 0}},
			wantReached: true,
		},
		{
			// The defect the first live run found. A consumer replacing a killed one is
			// never sent the records its predecessor committed, so a partition the
			// predecessor finished would hold the run open forever — reported as a
			// timeout, in the middle of an experiment about losing records.
			name:        "a partition its predecessor finished is already drained for the replacement",
			end:         map[int32]int64{0: 10, 1: 10},
			committed:   map[int32]int64{0: 10, 1: 6},
			reads:       []read{{1, 6}, {1, 7}, {1, 8}, {1, 9}},
			wantReached: true,
		},
		{
			name:          "the predecessor committed part of a partition and the rest is still owed",
			end:           map[int32]int64{0: 10},
			committed:     map[int32]int64{0: 6},
			wantRemaining: 4,
		},
		{
			name:        "a resumed consumer reads the tail it was left",
			end:         map[int32]int64{0: 10},
			reads:       []read{{0, 7}, {0, 8}, {0, 9}},
			wantReached: true,
		},
		{
			name:          "records arriving out of order do not walk the high-water mark backwards",
			end:           map[int32]int64{0: 4},
			reads:         []read{{0, 3}, {0, 1}},
			wantRemaining: 0,
			wantReached:   true,
		},
		{
			name:        "a topic with no partitions is drained, not stuck",
			end:         map[int32]int64{},
			wantReached: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := newProgress(tt.end, tt.committed)
			for _, r := range tt.reads {
				tracker.saw(r.partition, r.offset)
			}

			if got := tracker.reached(); got != tt.wantReached {
				t.Errorf("reached() = %v, want %v", got, tt.wantReached)
			}
			if got := tracker.remaining(); got != tt.wantRemaining {
				t.Errorf("remaining() = %d, want %d", got, tt.wantRemaining)
			}
		})
	}
}
