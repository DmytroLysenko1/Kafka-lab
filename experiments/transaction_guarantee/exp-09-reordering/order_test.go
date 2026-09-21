package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"
)

// The package builds kgo clients, whose goroutines are reaped only if Close runs to
// completion. No test here drives one yet, so this currently guards the next test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// sentOrder builds the events the producer sends for n payments, in the order it sends them.
func sentOrder(n int) []event {
	events := make([]event, 0, 2*n)
	for i := range n {
		id := string(rune('a' + i))
		events = append(events,
			event{PaymentID: id, Status: statusCaptured, Seq: 2 * i},
			event{PaymentID: id, Status: statusRefunded, Seq: 2*i + 1},
		)
	}
	return events
}

func reorder(events []event, order ...int) []event {
	out := make([]event, 0, len(order))
	for _, i := range order {
		out = append(out, events[i])
	}
	return out
}

func TestAnalyseReportsWhatTheClusterFailedToKeep(t *testing.T) {
	sent := sentOrder(3) // seq 0..5: a-captured, a-refunded, b-captured, b-refunded, c-captured, c-refunded

	tests := []struct {
		name      string
		delivered []event
		want      order
	}{
		{
			name:      "delivered as sent",
			delivered: sent,
			want:      order{Produced: 6, Read: 6, Distinct: 6},
		},
		{
			// The shape a retried batch leaves when a later one already landed: the first
			// batch arrives after the second.
			name:      "an earlier batch landing after a later one",
			delivered: reorder(sent, 2, 3, 0, 1, 4, 5),
			want:      order{Produced: 6, Read: 6, Distinct: 6, Inversions: 2},
		},
		{
			name:      "a refund recorded before the capture it refunds",
			delivered: reorder(sent, 1, 0, 2, 3, 4, 5),
			want:      order{Produced: 6, Read: 6, Distinct: 6, Inversions: 1, RefundedFirst: 1},
		},
		{
			// A batch rewound and resent after it already landed: its records arrive
			// twice, and the second copies are duplicates, not also inversions.
			name:      "a batch sent twice counts as duplicates and nothing else",
			delivered: append(append([]event{}, sent...), sent[2], sent[3]),
			want:      order{Produced: 6, Read: 8, Distinct: 6, Duplicates: 2},
		},
		{
			name:      "reordered and duplicated at the same failure edge",
			delivered: reorder(sent, 2, 3, 0, 1, 2, 3, 4, 5),
			want:      order{Produced: 6, Read: 8, Distinct: 6, Duplicates: 2, Inversions: 2},
		},
		{
			name:      "records that never arrived",
			delivered: sent[:4],
			want:      order{Produced: 6, Read: 4, Distinct: 4, Missing: 2},
		},
		{
			name:      "nothing delivered is not the same as everything in order",
			delivered: nil,
			want:      order{Produced: 6, Missing: 6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := analyse(tt.delivered, tt.want.Produced)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestHeldOnlyWhenNothingWasLostDuplicatedOrReordered(t *testing.T) {
	tests := []struct {
		name string
		o    order
		want bool
	}{
		{name: "clean", o: order{Produced: 10, Read: 10, Distinct: 10}, want: true},
		{name: "one inversion", o: order{Inversions: 1}, want: false},
		{name: "one duplicate", o: order{Duplicates: 1}, want: false},
		{name: "one refund first", o: order{RefundedFirst: 1}, want: false},
		{name: "one missing", o: order{Missing: 1}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.o.held(); got != tt.want {
				t.Errorf("held() = %v, want %v", got, tt.want)
			}
		})
	}
}
