package main

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// event is anything a member went through that is not handling a record.
type event struct {
	at     time.Duration
	member string
	what   string
}

type lagSample struct {
	at  time.Duration
	lag int64
}

// timeline collects what the members and the sampler saw. Members write from their own
// goroutines, group callbacks from franz-go's, so every access goes through the lock.
type timeline struct {
	mu             sync.Mutex
	start          time.Time
	seen           map[int64]int
	handlers       map[int64]map[string]bool
	events         []event
	lags           []lagSample
	failedSamples  int
	commitFailures int
	// firstOwned is when each member was first assigned at least one partition.
	firstOwned map[string]time.Duration
	// outside marks a member whose commit the group refused, until it is assigned
	// partitions again. While it is set, what the member handles and every commit that
	// still reports success are counted.
	outside         map[string]bool
	handledOutside  map[string]int
	acceptedOutside map[string]int
	// committed is what the last accepted commit stored, per partition, and rewinds the
	// accepted commits that stored less than it: the group's position moved backwards.
	committed map[int32]int64
	rewinds   []rewind
}

// rewind is an accepted commit that moved a partition's committed offset backwards.
type rewind struct {
	at        time.Duration
	member    string
	partition int32
	by        int64
}

func newTimeline(start time.Time) *timeline {
	return &timeline{
		start:           start,
		seen:            make(map[int64]int),
		handlers:        make(map[int64]map[string]bool),
		firstOwned:      make(map[string]time.Duration),
		outside:         make(map[string]bool),
		handledOutside:  make(map[string]int),
		acceptedOutside: make(map[string]int),
		committed:       make(map[int32]int64),
	}
}

func (t *timeline) handled(member string, seq int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seen[seq]++
	if t.handlers[seq] == nil {
		t.handlers[seq] = make(map[string]bool, 2)
	}
	t.handlers[seq][member] = true
	if t.outside[member] {
		t.handledOutside[member]++
	}
}

func (t *timeline) add(member, what string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event{at: time.Since(t.start), member: member, what: what})
}

func (t *timeline) change(member, kind string) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(_ context.Context, _ *kgo.Client, moved map[string][]int32) {
		t.add(member, fmt.Sprintf("%s %v", kind, slices.Sorted(slices.Values(moved[topic]))))
		if kind == "assigned" && len(moved[topic]) > 0 {
			t.owned(member)
		}
	}
}

func (t *timeline) commitAccepted(member string, stored map[int32]int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.outside[member] {
		t.acceptedOutside[member]++
	}
	for partition, offset := range stored {
		if last := t.committed[partition]; offset < last {
			t.rewinds = append(t.rewinds, rewind{at: time.Since(t.start), member: member, partition: partition, by: last - offset})
		}
		t.committed[partition] = offset
	}
}

func (t *timeline) owned(member string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.outside[member] = false
	if _, seen := t.firstOwned[member]; !seen {
		t.firstOwned[member] = time.Since(t.start)
	}
}

func (t *timeline) paused(member string) {
	t.add(member, "paused fetching, rewound, released the rebalance")
}

func (t *timeline) resumed(member string) {
	t.add(member, "dependency back, resumed fetching")
}

func (t *timeline) commitFailed(member string, err error) {
	t.mu.Lock()
	t.commitFailures++
	t.outside[member] = true
	t.mu.Unlock()
	t.add(member, fmt.Sprintf("commit refused: %v", err))
}

func (t *timeline) fetchFailed(member string, err error) {
	t.add(member, fmt.Sprintf("fetch error: %v", err))
}

func (t *timeline) sample(lag int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lags = append(t.lags, lagSample{at: time.Since(t.start), lag: lag})
}

func (t *timeline) sampleFailed() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failedSamples++
}

// recording is a finished timeline, copied out from under the lock.
type recording struct {
	seen            map[int64]int
	byBoth          int
	events          []event
	lags            []lagSample
	failedSamples   int
	commitFailures  int
	firstOwned      map[string]time.Duration
	handledOutside  map[string]int
	acceptedOutside map[string]int
	rewinds         []rewind
}

func (t *timeline) snapshot() recording {
	t.mu.Lock()
	defer t.mu.Unlock()
	byBoth := 0
	for _, members := range t.handlers {
		if len(members) > 1 {
			byBoth++
		}
	}
	return recording{seen: maps.Clone(t.seen), byBoth: byBoth, handledOutside: maps.Clone(t.handledOutside), acceptedOutside: maps.Clone(t.acceptedOutside), rewinds: slices.Clone(t.rewinds), events: slices.Clone(t.events), lags: slices.Clone(t.lags), failedSamples: t.failedSamples, commitFailures: t.commitFailures, firstOwned: maps.Clone(t.firstOwned)}
}

// tally checks every acknowledged record against what was handled. The producer numbers
// records from zero and fails the run if any was not acknowledged, so the expected set is
// exactly 0..produced-1.
type tally struct {
	missing, twice int
}

func count(seen map[int64]int, produced int64) tally {
	var got tally
	for seq := range produced {
		switch n := seen[seq]; {
		case n == 0:
			got.missing++
		case n > 1:
			got.twice++
		}
	}
	return got
}

// peak is the highest lag sampled and when.
func peak(lags []lagSample) lagSample {
	var top lagSample
	for _, s := range lags {
		if s.lag > top.lag {
			top = s
		}
	}
	return top
}

// drainedAt is the first sample after the outage at which the lag is back under limit.
func drainedAt(lags []lagSample, after time.Duration, limit int64) (time.Duration, bool) {
	for _, s := range lags {
		if s.at >= after && s.lag <= limit {
			return s.at, true
		}
	}
	return 0, false
}

func describe(handler string) string {
	if handler == handlerBlock {
		return "block: retries the dependency inside the batch, holding it and the rebalance"
	}
	return "pause: on failure pauses fetching, rewinds to the first unhandled record, releases the rebalance"
}

const drainedBelow = 100

func report(cfg *settings, group string, produced int64, r *recording) []string {
	outageEnd := cfg.outageAt + cfg.outageFor
	got := count(r.seen, produced)
	top := peak(r.lags)

	lines := []string{
		fmt.Sprintf("handler\t%s", describe(cfg.handler)),
		fmt.Sprintf("group\t%s", group),
		fmt.Sprintf("load\t%d records/s over %d partitions, %s", cfg.rate, partitions, cfg.produceFor),
		fmt.Sprintf("dependency down\t%s to %s", cfg.outageAt, outageEnd),
		fmt.Sprintf("second consumer joins\t%s", cfg.joinAt),
		fmt.Sprintf("rebalance timeout\t%s", rebalanceTimeout),
		"",
		"what the members went through",
	}
	for _, e := range r.events {
		lines = append(lines, fmt.Sprintf("  %s\t%s %s", e.at.Round(100*time.Millisecond), e.member, e.what))
	}
	lines = append(lines, "", "lag, sampled every second (every second sample shown)")
	for i, s := range r.lags {
		if i%2 == 0 {
			lines = append(lines, fmt.Sprintf("  %s\t%d", s.at.Round(time.Second), s.lag))
		}
	}
	drained := "not within the run"
	joined := "never"
	if at, ok := r.firstOwned["B"]; ok {
		joined = fmt.Sprintf("%s, %s after it joined", at.Round(100*time.Millisecond), (at - cfg.joinAt).Round(100*time.Millisecond))
	}
	if at, ok := drainedAt(r.lags, outageEnd, drainedBelow); ok {
		drained = fmt.Sprintf("%s, %s after the dependency came back", at.Round(time.Second), (at - outageEnd).Round(time.Second))
	}
	return append(lines, "",
		fmt.Sprintf("peak lag\t%d at %s", top.lag, top.at.Round(time.Second)),
		fmt.Sprintf("lag under %d again\t%s", drainedBelow, drained),
		fmt.Sprintf("second consumer first owned a partition\t%s", joined),
		fmt.Sprintf("commits refused by the group\t%d", r.commitFailures),
		fmt.Sprintf("produced and acknowledged\t%d", produced),
		fmt.Sprintf("  handled twice\t%d, %d of them by both members", got.twice, r.byBoth),
		"after the group refused a member's commit, until it was assigned again",
		fmt.Sprintf("  records it handled\t%s", perMember(r.handledOutside)),
		fmt.Sprintf("  commits that still reported success\t%s", perMember(r.acceptedOutside)),
		fmt.Sprintf("accepted commits that moved an offset backwards\t%s", describeRewinds(r.rewinds)),
		fmt.Sprintf("  never handled\t%d", got.missing),
		fmt.Sprintf("lag samples that failed\t%d", r.failedSamples),
	)
}

func render(out io.Writer, lines []string) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("exp-16: write report: %w", err)
		}
	}
	return table.Flush()
}

func perMember(after map[string]int) string {
	if len(after) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(after))
	for _, member := range slices.Sorted(maps.Keys(after)) {
		parts = append(parts, fmt.Sprintf("%s: %d", member, after[member]))
	}
	return strings.Join(parts, ", ")
}

func describeRewinds(rewinds []rewind) string {
	if len(rewinds) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(rewinds))
	for _, r := range rewinds {
		parts = append(parts, fmt.Sprintf("%s by %s on partition %d, back %d records", r.at.Round(100*time.Millisecond), r.member, r.partition, r.by))
	}
	return strings.Join(parts, "; ")
}
