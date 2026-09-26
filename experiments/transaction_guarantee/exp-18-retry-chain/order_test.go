package main

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"
)

var start = time.Date(2026, time.September, 26, 12, 0, 0, 0, time.UTC)

func at(milliseconds int) time.Time {
	return start.Add(time.Duration(milliseconds) * time.Millisecond)
}

// The chain's reordering is the point of the "ordering assumptions" claim, so what is
// counted as an overtake has to be exactly that: a later payment on the same partition
// counted before an earlier contended one — not any two payments that finished in some
// order, and not two on different partitions, which Kafka never ordered in the first place.
func TestReorderingCountsOnlyPaymentsThatOvertookAContendedOneOnItsPartition(t *testing.T) {
	rows := []counted{
		{eventID: 1, partition: 0, at: at(100)},
		{eventID: 2, partition: 0, hot: true, at: at(10000)},
		{eventID: 3, partition: 0, at: at(200)},
		{eventID: 4, partition: 1, at: at(300)},
		{eventID: 5, partition: 0, hot: true, at: at(9000)},
		{eventID: 6, partition: 0, at: at(11000)},
		{eventID: 7, partition: 1, hot: true, at: at(12000)},
		{eventID: 8, partition: 1, at: at(400)},
	}

	got := reorderingOf(rows)

	// 3 and 5 overtook 2 on partition 0; 4 is on another partition and does not count;
	// 8 overtook 7 on partition 1. 2 and 5 are the one same-partition contended pair, and
	// they swapped; 7 has no contended partner on its partition.
	want := reordering{overtakes: 3, hotInversions: 1, hotPairs: 1}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(reordering{})); diff != "" {
		t.Errorf("reordering (-want +got):\n%s", diff)
	}
}

// A stall is the time the healthy payments waited; the contended payment's own late count
// is not one, or the lock's hold would be read as the partition's wait.
func TestStallsAreGapsBetweenHealthyPaymentsOnly(t *testing.T) {
	rows := []counted{
		{eventID: 1, at: at(0)},
		{eventID: 2, at: at(10)},
		{eventID: 3, hot: true, at: at(30000)},
		{eventID: 4, at: at(2010)},
		{eventID: 5, at: at(2020)},
		{eventID: 6, at: at(6000)},
	}

	got := stalls(rows)

	want := []time.Duration{2 * time.Second, 3980 * time.Millisecond}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("stalls (-want +got):\n%s", diff)
	}
}

func TestEventIDIsReadFromItsHeaderAndARecordWithoutOneIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		headers []kgo.RecordHeader
		want    int64
		wantErr error
	}{
		{
			name:    "the header among others",
			headers: []kgo.RecordHeader{{Key: "trace_id", Value: []byte("x")}, {Key: "event_id", Value: []byte("42")}},
			want:    42,
		},
		{
			name:    "no event_id: refused, not read as event zero",
			headers: []kgo.RecordHeader{{Key: "trace_id", Value: []byte("x")}},
			wantErr: errNoEventID,
		},
		{
			name:    "an event_id that is not a number",
			headers: []kgo.RecordHeader{{Key: "event_id", Value: []byte("abc")}},
			wantErr: strconv.ErrSyntax,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := eventIDOf(tt.headers)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("event id = %d, want %d", got, tt.want)
			}
		})
	}
}
