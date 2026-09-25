package main

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// share is how many records one partition holds, and what fraction of the topic that is.
type share struct {
	Partition int32
	Records   int
	Percent   float64
}

// distribution turns "records per partition" into the shape an operator reads: sorted by
// weight, so the hot partition is the first row and the size of the skew is obvious.
func distribution(perPartition map[int32]int) []share {
	total := 0
	for _, records := range perPartition {
		total += records
	}

	shares := make([]share, 0, len(perPartition))
	for partition, records := range perPartition {
		percent := 0.0
		if total > 0 {
			percent = float64(records) * 100 / float64(total)
		}
		shares = append(shares, share{
			Partition: partition,
			Records:   records,
			Percent:   percent,
		})
	}

	slices.SortFunc(shares, func(left, right share) int {
		return cmp.Or(cmp.Compare(right.Records, left.Records),
			cmp.Compare(left.Partition, right.Partition))
	})
	return shares
}

// hottest is the share of the busiest partition: the floor under any drain time, because
// one partition is handled by exactly one consumer however many are in the group.
func hottest(perPartition map[int32]int) share {
	shares := distribution(perPartition)
	if len(shares) == 0 {
		return share{}
	}
	return shares[0]
}

// idle counts consumers that handled nothing: with fewer records than consumers to go
// round, or with all the weight on one partition, extra members simply wait.
func idle(perConsumer []int) int {
	count := 0
	for _, handled := range perConsumer {
		if handled == 0 {
			count++
		}
	}
	return count
}

func formatShares(shares []share) string {
	parts := make([]string, 0, len(shares))
	for _, share := range shares {
		parts = append(parts, fmt.Sprintf("p%d:%d (%.0f%%)", share.Partition, share.Records, share.Percent))
	}
	return strings.Join(parts, "  ")
}
