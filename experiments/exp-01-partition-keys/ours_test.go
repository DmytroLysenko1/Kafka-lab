package main

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fetched builds what one poll would hand back: records laid out on the partitions they
// arrived on, in the order the consumer would see them.
func fetched(t *testing.T, partitions map[int32][]event) kgo.Fetches {
	t.Helper()

	topic := kgo.FetchTopic{Topic: "exp01.keyless"}
	for partition := range 6 {
		events, ok := partitions[int32(partition)]
		if !ok {
			continue
		}

		records := make([]*kgo.Record, 0, len(events))
		for _, decoded := range events {
			value, err := json.Marshal(decoded)
			if err != nil {
				t.Fatalf("marshal %v: %v", decoded, err)
			}
			records = append(records, &kgo.Record{Partition: int32(partition), Value: value})
		}
		topic.Partitions = append(topic.Partitions, kgo.FetchPartition{Partition: int32(partition), Records: records})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{topic}}}
}

func TestOursKeepsThisRunAndNothingElse(t *testing.T) {
	mine := func(payment string, seq int) event {
		return event{RunID: "run-b", PaymentID: payment, Seq: seq}
	}
	theirs := func(payment string, seq int) event {
		return event{RunID: "run-a", PaymentID: payment, Seq: seq}
	}

	tests := []struct {
		name string
		args map[int32][]event
		want []processed
	}{
		{
			name: "a rerun reading a topic it shares with an earlier run keeps only its own events",
			args: map[int32][]event{
				0: {theirs("pay-1", 0), mine("pay-1", 0), theirs("pay-1", 1)},
				3: {mine("pay-1", 1), theirs("pay-2", 0)},
			},
			want: []processed{
				{PaymentID: "pay-1", Seq: 0, Partition: 0},
				{PaymentID: "pay-1", Seq: 1, Partition: 3},
			},
		},
		{
			name: "every event belongs to an earlier run: this run handled nothing",
			args: map[int32][]event{0: {theirs("pay-1", 0), theirs("pay-1", 1)}},
			want: []processed{},
		},
		{
			name: "the partition a record arrived on is carried through, because ordering is per partition",
			args: map[int32][]event{
				5: {mine("pay-9", 7)},
			},
			want: []processed{{PaymentID: "pay-9", Seq: 7, Partition: 5}},
		},
		{
			name: "an empty poll is not an error",
			args: map[int32][]event{},
			want: []processed{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ours(fetched(t, tt.args), "run-b")
			if err != nil {
				t.Fatalf("ours() err = %v, want nil", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A record this run cannot decode is not a record it may skip: skipping one would make the
// exact-count check pass while an event went uncounted.
func TestOursRefusesAnUndecodableRecord(t *testing.T) {
	broken := kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: "exp01.keyless",
		Partitions: []kgo.FetchPartition{{
			Partition: 2,
			Records:   []*kgo.Record{{Partition: 2, Offset: 41, Value: []byte("{not json")}},
		}},
	}}}}

	if _, err := ours(broken, "run-b"); !errors.Is(err, errUndecodable) {
		t.Fatalf("ours() err = %v, want %v", err, errUndecodable)
	}
}
