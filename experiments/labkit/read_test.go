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
		reads         []read
		wantDone      bool
		wantRemaining int64
	}{
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
			progress := newTracker(tt.extent)
			for _, r := range tt.reads {
				progress.saw(r.partition, r.offset)
			}
			if got := progress.done(); got != tt.wantDone {
				t.Errorf("done() = %v, want %v", got, tt.wantDone)
			}
			if got := progress.remaining(); got != tt.wantRemaining {
				t.Errorf("remaining() = %d, want %d", got, tt.wantRemaining)
			}
		})
	}
}
