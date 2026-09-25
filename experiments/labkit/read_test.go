package labkit

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
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

func TestOffsetAtRefusesEveryWayAListingDeclinesToAnswer(t *testing.T) {
	const topic = "exp03.retention"

	tests := []struct {
		name    string
		listed  kadm.ListedOffsets
		want    int64
		wantErr error
	}{
		{
			name:   "a partition that answered is read as its offset",
			listed: kadm.ListedOffsets{topic: {0: {Topic: topic, Partition: 0, Offset: 2000}}},
			want:   2000,
		},
		{
			name:   "an empty partition answers zero, and zero is an answer",
			listed: kadm.ListedOffsets{topic: {0: {Topic: topic, Partition: 0, Offset: 0}}},
			want:   0,
		},
		{
			// kadm answers a topic the cluster does not know with a synthetic partition -1,
			// so a lookup of partition 0 finds nothing and hands back a zero ListedOffset
			// beside ok=false. Read as a value, that is a log whose end offset is 0.
			name:    "a topic the cluster does not know has no partition 0 to report",
			listed:  kadm.ListedOffsets{topic: {-1: {Topic: topic, Partition: -1, Err: kerr.UnknownTopicOrPartition}}},
			wantErr: ErrNoOffsets,
		},
		{
			name:    "a partition carrying a load error is not an offset",
			listed:  kadm.ListedOffsets{topic: {0: {Topic: topic, Partition: 0, Err: kerr.NotLeaderForPartition}}},
			wantErr: ErrNoOffsets,
		},
		{
			// The answer a leader that has just been replaced gives while the new one is
			// still being elected.
			name:    "the -1 of an offset that could not be found is not an offset",
			listed:  kadm.ListedOffsets{topic: {0: {Topic: topic, Partition: 0, Offset: -1}}},
			wantErr: ErrNoOffsets,
		},
		{
			name:    "an empty response answers nothing",
			listed:  kadm.ListedOffsets{},
			wantErr: ErrNoOffsets,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OffsetAt(tt.listed, topic, 0)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("offset = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestOffsetAtKeepsTheBrokersOwnReasonReadable(t *testing.T) {
	const topic = "exp10d.stream"
	listed := kadm.ListedOffsets{topic: {0: {Topic: topic, Partition: 0, Err: kerr.UnknownTopicOrPartition}}}

	_, err := OffsetAt(listed, topic, 0)
	if !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Errorf("err = %v, want it to carry %v", err, kerr.UnknownTopicOrPartition)
	}
}

func TestSpansFromNeverReportsAnUnansweredTopicAsAnEmptyLog(t *testing.T) {
	const topic = "exp03.retention"

	tests := []struct {
		name    string
		starts  kadm.ListedOffsets
		ends    kadm.ListedOffsets
		want    map[int32]Span
		wantErr error
	}{
		{
			// The reading exp-03 publishes as its result: 2 000 records written, 0 readable
			// once retention has run. A listing that never answered produces the same
			// reading, and this is the only place that can tell the two apart.
			name:    "a topic missing from the response is refused, not read as a log of length zero",
			starts:  kadm.ListedOffsets{},
			ends:    kadm.ListedOffsets{},
			wantErr: ErrNoOffsets,
		},
		{
			name:    "a response holding only the unknown-topic partition is refused",
			starts:  kadm.ListedOffsets{topic: {-1: {Topic: topic, Partition: -1, Err: kerr.UnknownTopicOrPartition}}},
			ends:    kadm.ListedOffsets{topic: {-1: {Topic: topic, Partition: -1, Err: kerr.UnknownTopicOrPartition}}},
			wantErr: ErrNoOffsets,
		},
		{
			// The control this guard must not break: a log the broker positively reported
			// as empty is a real answer, and exp-03 relies on being able to read it.
			name:   "a log the broker reported as empty is a span, not an error",
			starts: kadm.ListedOffsets{topic: {0: {Topic: topic, Partition: 0, Offset: 0}}},
			ends:   kadm.ListedOffsets{topic: {0: {Topic: topic, Partition: 0, Offset: 0}}},
			want:   map[int32]Span{0: {Start: 0, End: 0}},
		},
		{
			// What retention actually leaves behind: the head is gone, so the log no longer
			// starts at zero, and every record between the two offsets is still readable.
			name:   "a log whose head retention deleted is measured from where it now starts",
			starts: kadm.ListedOffsets{topic: {0: {Topic: topic, Partition: 0, Offset: 2000}}},
			ends:   kadm.ListedOffsets{topic: {0: {Topic: topic, Partition: 0, Offset: 2010}}},
			want:   map[int32]Span{0: {Start: 2000, End: 2010}},
		},
		{
			name:   "every partition that answered becomes a span",
			starts: kadm.ListedOffsets{topic: {0: {Partition: 0, Offset: 0}, 1: {Partition: 1, Offset: 5}}},
			ends:   kadm.ListedOffsets{topic: {0: {Partition: 0, Offset: 10}, 1: {Partition: 1, Offset: 5}}},
			want:   map[int32]Span{0: {Start: 0, End: 10}, 1: {Start: 5, End: 5}},
		},
		{
			name:    "a partition the end listing reported and the start listing did not is refused",
			starts:  kadm.ListedOffsets{topic: {0: {Partition: 0, Offset: 0}}},
			ends:    kadm.ListedOffsets{topic: {0: {Partition: 0, Offset: 10}, 1: {Partition: 1, Offset: 10}}},
			wantErr: ErrNoOffsets,
		},
		{
			name:    "a load error on one partition refuses the whole listing",
			starts:  kadm.ListedOffsets{topic: {0: {Partition: 0, Offset: 0}}},
			ends:    kadm.ListedOffsets{topic: {0: {Partition: 0, Err: kerr.NotLeaderForPartition}}},
			wantErr: ErrNoOffsets,
		},
		{
			name:    "a log that ends before it starts is refused",
			starts:  kadm.ListedOffsets{topic: {0: {Partition: 0, Offset: 10}}},
			ends:    kadm.ListedOffsets{topic: {0: {Partition: 0, Offset: 4}}},
			wantErr: ErrNoOffsets,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := spansFrom(topic, tt.starts, tt.ends)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("spans mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The consequence the guard above exists to prevent, spelled out: a reader built from a
// listing that never answered is finished before it polls once, so the experiment reports
// a log it never read as a log that is empty. Tracker is right to call an empty extent
// finished — that is arithmetic — which is why the refusal has to happen upstream of it.
func TestAnUnansweredListingCannotProduceAReaderThatIsAlreadyFinished(t *testing.T) {
	const topic = "exp03.retention"

	if _, err := spansFrom(topic, kadm.ListedOffsets{}, kadm.ListedOffsets{}); !errors.Is(err, ErrNoOffsets) {
		t.Fatalf("err = %v, want %v", err, ErrNoOffsets)
	}

	if !NewTracker(map[int32]Span{}).Done() {
		t.Fatal("an empty extent no longer reads as a finished log; this test's premise is stale")
	}
}
