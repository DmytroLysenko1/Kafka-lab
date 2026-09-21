package labkit

import (
	"testing"

	"go.uber.org/goleak"
)

// ReadAll builds kgo clients whose goroutines are reaped only if Close runs to completion.
// No test here drives one yet, so this currently guards the next test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestTrackerKnowsWhenTheWholeLogHasBeenRead(t *testing.T) {
	type read struct {
		partition int32
		offset    int64
	}

	tests := []struct {
		name          string
		extent        map[int32]Span
		committed     map[int32]int64
		reads         []read
		wantDone      bool
		wantRemaining int64
	}{
		{
			// The defect exp-05's first live run found. A consumer replacing a killed one
			// is never sent the records its predecessor committed, so a partition the
			// predecessor finished would hold the run open forever.
			name:      "a partition a predecessor finished is already read for its replacement",
			extent:    map[int32]Span{0: {Start: 0, End: 10}, 1: {Start: 0, End: 10}},
			committed: map[int32]int64{0: 10, 1: 6},
			reads:     []read{{1, 6}, {1, 7}, {1, 8}, {1, 9}},
			wantDone:  true,
		},
		{
			name:          "a predecessor committed part of a partition and the rest is still owed",
			extent:        map[int32]Span{0: {Start: 0, End: 10}},
			committed:     map[int32]int64{0: 6},
			wantRemaining: 4,
		},
		{
			// A transactional log ends in a commit or abort marker. A read_committed
			// reader is only handed it with control records kept; without that the last
			// offset is never seen and the read would end in a false short-read error.
			name:     "a transaction marker at the last offset finishes the partition",
			extent:   map[int32]Span{0: {Start: 0, End: 4}},
			reads:    []read{{0, 0}, {0, 1}, {0, 2}, {0, 3}},
			wantDone: true,
		},
		{
			name:          "nothing read",
			extent:        map[int32]Span{0: {Start: 0, End: 3}},
			wantRemaining: 3,
		},
		{
			name:     "an empty log is read by definition, not a hang",
			extent:   map[int32]Span{0: {Start: 5, End: 5}},
			wantDone: true,
		},
		{
			// A log whose head was deleted by retention starts past zero; counting from
			// zero would wait for offsets that no longer exist.
			name:          "a log that no longer starts at zero is measured from where it starts",
			extent:        map[int32]Span{0: {Start: 2000, End: 2003}},
			reads:         []read{{0, 2000}},
			wantRemaining: 2,
		},
		{
			// A compacted log has gaps, so the count of records read is not the measure:
			// reaching the last offset is.
			name:     "a compacted log with gaps is finished at its last offset",
			extent:   map[int32]Span{0: {Start: 0, End: 10}},
			reads:    []read{{0, 3}, {0, 9}},
			wantDone: true,
		},
		{
			name:          "one partition finished and one not is not finished",
			extent:        map[int32]Span{0: {Start: 0, End: 2}, 1: {Start: 0, End: 2}},
			reads:         []read{{0, 0}, {0, 1}},
			wantRemaining: 2,
		},
		{
			name:     "an offset seen twice does not count twice",
			extent:   map[int32]Span{0: {Start: 0, End: 2}},
			reads:    []read{{0, 0}, {0, 1}, {0, 1}},
			wantDone: true,
		},
		{
			name:     "a topic with no partitions has nothing to read",
			extent:   map[int32]Span{},
			wantDone: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			progress := NewTracker(tt.extent)
			for partition, next := range tt.committed {
				progress.Seed(partition, next)
			}
			for _, r := range tt.reads {
				progress.Saw(r.partition, r.offset)
			}
			if got := progress.Done(); got != tt.wantDone {
				t.Errorf("Done() = %v, want %v", got, tt.wantDone)
			}
			if got := progress.Remaining(); got != tt.wantRemaining {
				t.Errorf("Remaining() = %d, want %d", got, tt.wantRemaining)
			}
		})
	}
}
