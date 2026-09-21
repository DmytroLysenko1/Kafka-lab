package main

import (
	"fmt"
	"time"
)

// stall is what the run observed.
type stall struct {
	Stable      int64
	Newest      int64
	Uncommitted time.Duration
	Committed   time.Duration
}

// held decides whether the run showed the stall rather than noise. The read_committed wait
// has to exceed the read_uncommitted one by at least half the transaction timeout: that
// ties the claim to the setting under test rather than to a constant, and a stall of that
// size cannot come from fetch latency, which the uncommitted reader measures in the same
// run.
func (s stall) held(timeout time.Duration) bool {
	return s.Newest > s.Stable && s.Committed-s.Uncommitted >= timeout/2
}

func report(cfg *settings, s stall) []string {
	verdict := "as expected"
	if !s.held(cfg.txnTimeout) {
		verdict = "NOT DEMONSTRATED — read_committed was not held back by the open transaction"
	}
	return []string{
		fmt.Sprintf("stuck transaction\tone record, never ended, transaction.timeout.ms %s", cfg.txnTimeout),
		fmt.Sprintf("unrelated records\t%d, written after it by a producer with no transaction", cfg.records),
		fmt.Sprintf("high watermark\t%d", s.Newest),
		fmt.Sprintf("last stable offset\t%d — %d records reported as lag and not deliverable", s.Stable, s.Newest-s.Stable),
		fmt.Sprintf("read_uncommitted saw them after\t%s", s.Uncommitted.Round(100*time.Millisecond)),
		fmt.Sprintf("read_committed saw them after\t%s", s.Committed.Round(100*time.Millisecond)),
		fmt.Sprintf("claim\tread_committed held back until the stuck transaction was aborted — %s", verdict),
	}
}
