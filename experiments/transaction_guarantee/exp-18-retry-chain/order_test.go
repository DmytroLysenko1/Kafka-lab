package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var start = time.Date(2026, time.September, 26, 12, 0, 0, 0, time.UTC)

func at(milliseconds int) time.Time {
	return start.Add(time.Duration(milliseconds) * time.Millisecond)
}

// The chain's reordering is the point of the "ordering assumptions" claim, so what is
// counted as an overtake has to be exactly that: a later payment counted before an earlier
// contended one — not any two payments that finished in some order.
func TestReorderingCountsOnlyPaymentsThatOvertookAContendedOne(t *testing.T) {
	rows := []counted{
		{eventID: 1, at: at(100)},
		{eventID: 2, hot: true, at: at(10000)},
		{eventID: 3, at: at(200)},
		{eventID: 4, at: at(300)},
		{eventID: 5, hot: true, at: at(9000)},
		{eventID: 6, at: at(11000)},
	}

	got := reorderingOf(rows)

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
