package main

import (
	"slices"
	"time"
)

// latencies is what the producer's callbacks observed: from the moment a record was handed
// to the client to the moment its acknowledgement came back.
type latencies []time.Duration

// percentile uses the nearest-rank definition: the smallest observation at or above p of
// the sample. It never interpolates, so every figure it reports is a latency some record
// actually had.
func (l latencies) percentile(p float64) time.Duration {
	if len(l) == 0 {
		return 0
	}
	sorted := slices.Clone(l)
	slices.Sort(sorted)

	rank := int(p*float64(len(sorted)) + 0.999999999)
	return sorted[min(max(rank, 1), len(sorted))-1]
}

// wire is what the client actually sent, summed from its per-batch metrics.
type wire struct {
	Batches      int
	Records      int
	Uncompressed int
	Compressed   int
}

func (w wire) ratio() float64 {
	if w.Compressed == 0 {
		return 0
	}
	return float64(w.Uncompressed) / float64(w.Compressed)
}

func (w wire) recordsPerBatch() float64 {
	if w.Batches == 0 {
		return 0
	}
	return float64(w.Records) / float64(w.Batches)
}
