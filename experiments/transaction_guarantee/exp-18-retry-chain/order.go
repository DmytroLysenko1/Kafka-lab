package main

import (
	"slices"
	"time"
)

// stallThreshold is the gap between two counted payments that is a wait, not work: the
// consumer counts a payment in a few milliseconds, and a lock wait is two seconds.
const stallThreshold = 300 * time.Millisecond

// counted is one inbox row: which payment, the partition it was produced to, whether it was
// the locked merchant's, and when the consumer counted it. The event id is the order the
// payments were produced in.
type counted struct {
	eventID   int64
	partition int32
	hot       bool
	at        time.Time
}

// reordering is what the chain does to the order payments were produced in, within a
// partition — the only order Kafka keeps. It reorders on purpose — a contended payment steps
// aside and the ones behind it go first — and this is how much.
type reordering struct {
	// overtakes is the number of (earlier contended payment, later payment on the same
	// partition) pairs where the later one was counted first.
	overtakes int
	// hotInversions is the number of same-partition pairs of contended payments counted in
	// the opposite order to the one they were produced in: whether the chain kept their own
	// order.
	hotInversions int
	hotPairs      int
}

// follows reports whether this payment was produced after earlier into the same partition:
// the only pairs Kafka promises an order for.
func (c counted) follows(earlier counted) bool {
	return c.partition == earlier.partition && c.eventID > earlier.eventID
}

func reorderingOf(rows []counted) reordering {
	var result reordering
	for _, earlier := range rows {
		if earlier.hot {
			result.against(earlier, rows)
		}
	}
	return result
}

// against counts, for one contended payment, every payment produced after it to the same
// partition that was counted before it, and every such contended one that kept or broke
// their order.
func (r *reordering) against(earlier counted, rows []counted) {
	for _, later := range rows {
		if !later.follows(earlier) {
			continue
		}
		overtook := later.at.Before(earlier.at)
		if overtook {
			r.overtakes++
		}
		if later.hot {
			r.hotPairs++
			if overtook {
				r.hotInversions++
			}
		}
	}
}

// stalls are the gaps between consecutive healthy payments long enough to be a wait: the
// time the payments behind a contended one spent waiting for its lock wait to run out.
func stalls(rows []counted) []time.Duration {
	healthy := slices.DeleteFunc(slices.Clone(rows), func(row counted) bool { return row.hot })
	slices.SortFunc(healthy, func(a, b counted) int { return a.at.Compare(b.at) })

	var gaps []time.Duration
	for i := 1; i < len(healthy); i++ {
		if gap := healthy[i].at.Sub(healthy[i-1].at); gap >= stallThreshold {
			gaps = append(gaps, gap)
		}
	}
	return gaps
}
