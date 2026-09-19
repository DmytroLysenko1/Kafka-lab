package main

// processed is one event as the consumer handled it, in handling order.
type processed struct {
	PaymentID string
	Seq       int
	Partition int32
}

// countViolations counts events that arrived after a later event of the same payment had
// already been handled. It reads the slice in handling order, which is the order the
// business sees — not the offsets, which are only ordered inside one partition.
func countViolations(events []processed) int {
	highest := make(map[string]int, len(events))

	violations := 0
	for _, event := range events {
		previous, seen := highest[event.PaymentID]
		switch {
		case !seen || event.Seq > previous:
			highest[event.PaymentID] = event.Seq
		case event.Seq < previous:
			violations++
		}
	}
	return violations
}

// scatteredPayments counts payments whose events did not all land in one partition. With a
// key that number is zero by construction; without one it is the mechanism behind every
// violation above.
func scatteredPayments(events []processed) int {
	partitions := make(map[string]map[int32]struct{}, len(events))
	for _, event := range events {
		seen, ok := partitions[event.PaymentID]
		if !ok {
			seen = make(map[int32]struct{}, 1)
			partitions[event.PaymentID] = seen
		}
		seen[event.Partition] = struct{}{}
	}

	scattered := 0
	for _, seen := range partitions {
		if len(seen) > 1 {
			scattered++
		}
	}
	return scattered
}
