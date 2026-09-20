package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSummariseReadsTheLogTheWayAConsumerDoes(t *testing.T) {
	tests := []struct {
		name    string
		records []record
		want    summary
	}{
		{
			name:    "an uncompacted log: every update still there, one key alive",
			records: []record{{Key: "state-1"}, {Key: "state-1"}, {Key: "state-1"}},
			want:    summary{Records: 3, LiveKeys: 1},
		},
		{
			name:    "a compacted log: one record per key",
			records: []record{{Key: "state-1"}, {Key: "state-2"}},
			want:    summary{Records: 2, LiveKeys: 2},
		},
		{
			name: "a deleted key is not alive, however many updates came before it",
			// This is the rule compaction preserves: the last record decides, and a
			// tombstone is a record.
			records: []record{{Key: "state-1"}, {Key: "state-1", Tombstone: true}},
			want:    summary{Records: 2, LiveKeys: 0, Tombstones: 1},
		},
		{
			name:    "a key written again after its tombstone is alive once more",
			records: []record{{Key: "state-1", Tombstone: true}, {Key: "state-1"}},
			want:    summary{Records: 2, LiveKeys: 1, Tombstones: 1},
		},
		{
			name: "the experiment's own segment-roll markers are counted apart",
			records: []record{
				{Key: "state-1"},
				{Key: rollKey},
				{Key: rollKey},
			},
			want: summary{Records: 3, LiveKeys: 1, Rolls: 2},
		},
		{
			name:    "an empty log",
			records: nil,
			want:    summary{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, summarise(tt.records)); diff != "" {
				t.Errorf("summary mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
