package main

import (
	"context"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The windows the gaps are measured in. The baseline is the steady state before the join,
// the second window starts just before it and runs long enough for any strategy to settle.
const (
	baselineWindow  = 10 * time.Second
	joinMargin      = 2 * time.Second
	afterJoinWindow = 15 * time.Second
	stoppedAfter    = time.Second
	// revokeCommitWait bounds the commit inside the revoke callback. The callback holds the
	// rebalance, and a member polling with BlockRebalanceOnPoll waits for it on a condition
	// variable that no context can interrupt — so the bound has to be here.
	revokeCommitWait = 5 * time.Second
)

// handling is one record handled by one member, at a time measured from the run's start.
type handling struct {
	at        time.Duration
	member    string
	partition int32
	seq       int64
}

// change is one assignment callback the group put a member through.
type change struct {
	at         time.Duration
	member     string
	kind       string
	partitions []int32
}

// timeline collects what both members did. The consumers write from their own goroutines,
// and the group's callbacks from franz-go's, so every access goes through the lock.
type timeline struct {
	mu         sync.Mutex
	start      time.Time
	measuredAt time.Duration
	log        []handling
	changes    []change
}

func newTimeline(start time.Time) *timeline {
	return &timeline{
		start: start,
	}
}

// markMeasured records the moment the report's windows are placed around: the second
// member joining, or leaving when the run is about a departure.
func (t *timeline) markMeasured() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.measuredAt = time.Since(t.start)
}

func (t *timeline) handled(member string, partition int32, seq int64, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.log = append(t.log, handling{
		at:        at.Sub(t.start),
		member:    member,
		partition: partition,
		seq:       seq,
	})
}

func (t *timeline) change(member, kind string) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
		t.record(member, kind, assigned[topic])
	}
}

// revoked replaces franz-go's default revoke callback, so it has to do what that callback
// does: commit what was handled, synchronously, before the partitions go. Without it the
// new owner restarts from the last autocommit and handles records twice — which the first
// version of this experiment reported as a property of cooperative rebalancing. Only marked
// records are committed, so a revoke that lands while the handler is still working cannot
// commit what nobody has handled.
func (t *timeline) revoked(member string) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(ctx context.Context, client *kgo.Client, revoked map[string][]int32) {
		ctx, cancel := context.WithTimeout(ctx, revokeCommitWait)
		defer cancel()
		kind := "revoked"
		if err := client.CommitMarkedOffsets(ctx); err != nil {
			kind = fmt.Sprintf("revoked, commit failed: %v", err)
		}
		t.record(member, kind, revoked[topic])
	}
}

func (t *timeline) record(member, kind string, partitions []int32) {
	moved := slices.Sorted(slices.Values(partitions))
	t.mu.Lock()
	defer t.mu.Unlock()
	t.changes = append(t.changes, change{
		at:         time.Since(t.start),
		member:     member,
		kind:       kind,
		partitions: moved,
	})
}

// recording is a finished timeline, copied out from under the lock.
type recording struct {
	measuredAt time.Duration
	handled    []handling
	changes    []change
}

func (t *timeline) snapshot() recording {
	t.mu.Lock()
	defer t.mu.Unlock()
	return recording{
		measuredAt: t.measuredAt,
		handled:    slices.Clone(t.log),
		changes:    slices.Clone(t.changes),
	}
}

// longestGaps is, per partition, the longest stretch inside [from, to) in which nothing of
// that partition was handled, counting the window's edges: a partition that stopped and
// never resumed inside the window is charged up to its end.
func longestGaps(handled []handling, from, to time.Duration) map[int32]time.Duration {
	last := make(map[int32]time.Duration, partitions)
	gaps := make(map[int32]time.Duration, partitions)
	for p := range int32(partitions) {
		last[p] = from
	}
	for _, h := range sortedByTime(handled) {
		if h.at < from || h.at >= to {
			continue
		}
		gaps[h.partition] = max(gaps[h.partition], h.at-last[h.partition])
		last[h.partition] = h.at
	}
	for p, at := range last {
		gaps[p] = max(gaps[p], to-at)
	}
	return gaps
}

// groupGap is the longest stretch inside [from, to) in which the whole group handled nothing.
func groupGap(handled []handling, from, to time.Duration) time.Duration {
	last, longest := from, time.Duration(0)
	for _, h := range sortedByTime(handled) {
		if h.at < from || h.at >= to {
			continue
		}
		longest = max(longest, h.at-last)
		last = h.at
	}
	return max(longest, to-last)
}

// handledTwice counts records handled more than once, by either member.
func handledTwice(handled []handling) int {
	seen := make(map[int64]int, len(handled))
	twice := 0
	for _, h := range handled {
		seen[h.seq]++
		if seen[h.seq] == 2 {
			twice++
		}
	}
	return twice
}

// missedInside counts records nobody handled although the group handled records on both
// sides of them. Records before the first consumer started and after the run ended are
// outside every partition's own first-to-last span and are not charged: what this catches
// is a gap, which is what a commit of unhandled records would leave behind.
func missedInside(handled []handling) int {
	seen := make(map[int32]map[int64]bool)
	for _, h := range handled {
		if seen[h.partition] == nil {
			seen[h.partition] = make(map[int64]bool)
		}
		seen[h.partition][h.seq] = true
	}

	missed := 0
	for _, seqs := range seen {
		lowest, highest := int64(math.MaxInt64), int64(math.MinInt64)
		for seq := range seqs {
			lowest, highest = min(lowest, seq), max(highest, seq)
		}
		// One partition carries every partitions-th record, so the span holds that many.
		missed += int((highest-lowest)/partitions+1) - len(seqs)
	}
	return missed
}

func sortedByTime(handled []handling) []handling {
	sorted := slices.Clone(handled)
	slices.SortStableFunc(sorted, func(a, b handling) int {
		return int(a.at - b.at)
	})
	return sorted
}

func report(cfg *settings, group string, r recording) []string {
	baseFrom, baseTo := r.measuredAt-joinMargin-baselineWindow, r.measuredAt-joinMargin
	joinFrom, joinTo := r.measuredAt-joinMargin, r.measuredAt+afterJoinWindow
	before := longestGaps(r.handled, baseFrom, baseTo)
	after := longestGaps(r.handled, joinFrom, joinTo)

	lines := []string{
		fmt.Sprintf("strategy\t%s", describe(cfg.strategy)),
		fmt.Sprintf("group\t%s", group),
		fmt.Sprintf("load\t%d records/s over %d partitions", cfg.rate, partitions),
		fmt.Sprintf("handler\t%s", describeHandler(cfg)),
		fmt.Sprintf("membership\t%s", describeMembership(cfg)),
		fmt.Sprintf("%s\t%s", measuredEvent(cfg), r.measuredAt.Round(10*time.Millisecond)),
		"",
		"assignment changes after the first",
	}
	for _, c := range r.changes {
		if c.at < r.measuredAt {
			continue
		}
		lines = append(lines, fmt.Sprintf("  %s\t%s %s %v", c.at.Round(10*time.Millisecond), c.member, c.kind, c.partitions))
	}
	lines = append(lines, "", fmt.Sprintf("partition\tlongest gap, %s before the join\tlongest gap, from %s before to %s after", baselineWindow, joinMargin, afterJoinWindow))
	stopped := 0
	for _, p := range slices.Sorted(maps.Keys(after)) {
		if after[p] > stoppedAfter {
			stopped++
		}
		lines = append(lines, fmt.Sprintf("  %d\t%s\t%s", p, before[p].Round(time.Millisecond), after[p].Round(time.Millisecond)))
	}
	return append(lines, "",
		fmt.Sprintf("partitions stopped for more than %s\t%d of %d", stoppedAfter, stopped, partitions),
		fmt.Sprintf("longest time the whole group handled nothing\t%s", groupGap(r.handled, joinFrom, joinTo).Round(time.Millisecond)),
		fmt.Sprintf("records handled twice\t%d", handledTwice(r.handled)),
		fmt.Sprintf("records missed between the first and last handled\t%d", missedInside(r.handled)),
	)
}

func render(out io.Writer, lines []string) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("exp-14: write report: %w", err)
		}
	}
	return table.Flush()
}

func describeMembership(cfg *settings) string {
	if cfg.static {
		return fmt.Sprintf("static, group.instance.id per member, session timeout %s", cfg.sessionTimeout)
	}
	return fmt.Sprintf("dynamic, session timeout %s", cfg.sessionTimeout)
}

func measuredEvent(cfg *settings) string {
	if cfg.leaveAfter > 0 {
		return "second consumer left at"
	}
	return "second consumer joined at"
}
