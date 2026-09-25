package main

import "sort"

// partition is one partition as the cluster describes it right now.
type partition struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
}

// preferred is the first replica in the assignment: the broker Kafka returns leadership to
// when it is asked to, and the one it never returns to on its own once auto rebalance is off.
func (p partition) preferred() int32 {
	if len(p.Replicas) == 0 {
		return -1
	}
	return p.Replicas[0]
}

func (p partition) underReplicated() bool {
	return len(p.Replicas) > 0 && len(p.ISR) < len(p.Replicas)
}

func (p partition) leaderless() bool {
	return p.Leader < 0
}

// health is the cluster's answer to "can I still write, and is anything degraded".
type health struct {
	Partitions      int
	Replicas        int
	UnderReplicated int
	Leaderless      int
	OffPreferred    int
	SmallestISR     int
}

func inspect(partitions []partition) health {
	state := health{
		Partitions:  len(partitions),
		SmallestISR: -1,
	}
	for _, part := range partitions {
		state.count(part)
	}
	return state
}

func (h *health) count(p partition) {
	h.Replicas = max(h.Replicas, len(p.Replicas))
	if p.underReplicated() {
		h.UnderReplicated++
	}
	switch {
	case p.leaderless():
		h.Leaderless++
	case p.Leader != p.preferred():
		h.OffPreferred++
	}
	if h.SmallestISR < 0 || len(p.ISR) < h.SmallestISR {
		h.SmallestISR = len(p.ISR)
	}
}

// leaders lists which broker leads which partition, in partition order, so two observations
// can be compared line by line.
func leaders(partitions []partition) []int32 {
	ordered := make([]partition, len(partitions))
	copy(ordered, partitions)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].ID < ordered[j].ID
	})

	result := make([]int32, 0, len(ordered))
	for _, part := range ordered {
		result = append(result, part.Leader)
	}
	return result
}
