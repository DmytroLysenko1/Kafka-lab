package main

import (
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Which partition a keyed record lands on is the client's arithmetic, not the broker's: the
// key's hash modulo the partition count. So what adding partitions does to existing keys is
// checked here, on the partitioner the client uses by default, without a cluster.
func TestAddingPartitionsMovesKeysTheirOrderingDependsOn(t *testing.T) {
	const keys = 10_000

	tests := []struct {
		name         string
		from, to     int
		wantMovedPct int
	}{
		// A key stays only when its hash leaves the same remainder by 6 and by 7: one in seven.
		{name: "one partition added to six", from: 6, to: 7, wantMovedPct: 86},
		// Doubling keeps every key whose remainder by 12 is below 6: half of them.
		{name: "six partitions doubled to twelve", from: 6, to: 12, wantMovedPct: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partitioner := keyedPartitioner(t)

			moved := 0
			for i := range keys {
				record := &kgo.Record{
					Key: fmt.Appendf(nil, "pay-%05d", i),
				}
				if partitioner.PartitionByBackup(record, tt.from, nil) != partitioner.PartitionByBackup(record, tt.to, nil) {
					moved++
				}
			}

			const tolerancePct = 3
			movedPct := moved * 100 / keys
			if movedPct < tt.wantMovedPct-tolerancePct || movedPct > tt.wantMovedPct+tolerancePct {
				t.Errorf("%d → %d partitions moved %d of %d keys (%d%%), want about %d%%",
					tt.from, tt.to, moved, keys, movedPct, tt.wantMovedPct)
			}
		})
	}
}

// keyedPartitioner is franz-go's default partitioner, built the way its default config builds
// it. A keyed record never reaches the backup iterator, so nil stands in for it.
func keyedPartitioner(t *testing.T) kgo.TopicBackupPartitioner {
	t.Helper()

	partitioner, ok := kgo.UniformBytesPartitioner(64<<10, true, true, nil).ForTopic("exp01.keyed").(kgo.TopicBackupPartitioner)
	if !ok {
		t.Fatal("the default partitioner no longer partitions by backup; re-read kgo/partitioner.go")
	}
	return partitioner
}
