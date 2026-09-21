package main

import "testing"

func TestCountViolationsReadsBusinessOrderNotArrivalCount(t *testing.T) {
	tests := []struct {
		name   string
		events []processed
		want   int
	}{
		{
			name:   "one payment handled in order",
			events: order("pay-1", 0, 1, 2, 3),
			want:   0,
		},
		{
			name:   "one payment whose third event overtook the second",
			events: order("pay-1", 0, 2, 1, 3),
			want:   1,
		},
		{
			name: "two payments interleaved, each in its own order",
			events: append(
				order("pay-1", 0, 1),
				order("pay-2", 0, 1)...,
			),
			want: 0,
		},
		{
			name: "a payment reversed entirely counts every event after the first",
			// 3 is handled first, so 2, 1 and 0 each arrive after a later event.
			events: order("pay-1", 3, 2, 1, 0),
			want:   3,
		},
		{
			name: "a duplicate of the newest event is not a violation",
			// At-least-once delivery repeats records; repeating the head of the sequence
			// does not break the order, and counting it would inflate every run.
			events: order("pay-1", 0, 1, 1, 2),
			want:   0,
		},
		{
			name:   "no events at all",
			events: nil,
			want:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := countViolations(tt.events); got != tt.want {
				t.Errorf("violations = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestScatteredPaymentsCountsPaymentsWhoseEventsSplit(t *testing.T) {
	tests := []struct {
		name   string
		events []processed
		want   int
	}{
		{
			name:   "every event of the payment in one partition",
			events: []processed{{PaymentID: "pay-1", Partition: 3}, {PaymentID: "pay-1", Partition: 3}},
			want:   0,
		},
		{
			name:   "one payment across two partitions",
			events: []processed{{PaymentID: "pay-1", Partition: 3}, {PaymentID: "pay-1", Partition: 5}},
			want:   1,
		},
		{
			name: "two payments, only one of them split",
			events: []processed{
				{PaymentID: "pay-1", Partition: 0},
				{PaymentID: "pay-2", Partition: 1},
				{PaymentID: "pay-2", Partition: 4},
			},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := scatteredPayments(tt.events); got != tt.want {
				t.Errorf("scattered payments = %d, want %d", got, tt.want)
			}
		})
	}
}

func order(payment string, seqs ...int) []processed {
	events := make([]processed, 0, len(seqs))
	for _, seq := range seqs {
		events = append(events, processed{PaymentID: payment, Seq: seq})
	}
	return events
}
