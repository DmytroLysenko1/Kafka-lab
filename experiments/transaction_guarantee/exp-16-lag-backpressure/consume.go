package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	handlerBlock = "block"
	handlerPause = "pause"

	// work is what a successful call to the dependency costs, and callTimeout what a
	// failing one costs before the caller gives up on it.
	work        = time.Millisecond
	callTimeout = 50 * time.Millisecond
	// backoff is how long a handler waits between attempts, in either shape.
	backoff = 500 * time.Millisecond
)

// dependency is the downstream service every record has to reach, down for [from, until)
// of the run.
type dependency struct {
	start       time.Time
	from, until time.Duration
}

func (d dependency) call(ctx context.Context) bool {
	now := time.Since(d.start)
	if now >= d.from && now < d.until {
		sleep(ctx, callTimeout)
		return false
	}
	sleep(ctx, work)
	return true
}

// consume is one member of the group. Both shapes hold rebalances off while they handle a
// batch, as the Java consumer does by rebalancing only inside poll, and commit only what
// they handled. They differ in what they do when the dependency fails.
func consume(ctx context.Context, cfg *settings, group, member string, dep dependency, line *timeline) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(rebalanceTimeout),
		kgo.OnPartitionsAssigned(line.change(member, "assigned")),
		kgo.OnPartitionsRevoked(line.change(member, "revoked")),
		kgo.OnPartitionsLost(line.change(member, "lost")),
	)
	if err != nil {
		return fmt.Errorf("exp-16: consumer %s: %w", member, err)
	}
	defer client.Close()
	// Runs before Close: a rebalance held off by an unfinished batch would otherwise hold
	// Close's leave forever.
	defer client.AllowRebalance()

	h := handler{client: client, member: member, dep: dep, line: line}
	for {
		fetches := client.PollRecords(ctx, pollRecords)
		if ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(_ string, _ int32, err error) { line.fetchFailed(member, err) })

		records := fetches.Records()
		if cfg.handler == handlerBlock {
			h.retryInline(ctx, records)
		} else {
			h.pauseAndRewind(ctx, records)
		}
		client.AllowRebalance()
	}
}

type handler struct {
	client *kgo.Client
	member string
	dep    dependency
	line   *timeline
}

// retryInline is the handler everyone writes first: keep calling the dependency until it
// answers. While it waits it holds the batch, and so it holds the rebalance.
func (h handler) retryInline(ctx context.Context, records []*kgo.Record) {
	for _, record := range records {
		for !h.dep.call(ctx) {
			if !sleep(ctx, backoff) {
				return
			}
		}
		h.handled(record)
	}
	h.commit(ctx, records)
}

// pauseAndRewind is backpressure: on the first failure it stops fetching, moves each
// partition back to its first unhandled record, commits what it did handle and lets the
// rebalance through. It waits for the dependency outside any batch, holding nothing.
func (h handler) pauseAndRewind(ctx context.Context, records []*kgo.Record) {
	for i, record := range records {
		if h.dep.call(ctx) {
			h.handled(record)
			continue
		}
		h.commit(ctx, records[:i])
		h.client.SetOffsets(rewindTo(records[i:]))
		h.client.PauseFetchPartitions(allPartitions())
		h.line.paused(h.member)
		h.client.AllowRebalance()
		for !h.dep.call(ctx) {
			if !sleep(ctx, backoff) {
				return
			}
		}
		h.client.ResumeFetchPartitions(allPartitions())
		h.line.resumed(h.member)
		return
	}
	h.commit(ctx, records)
}

func (h handler) handled(record *kgo.Record) {
	seq, err := strconv.ParseInt(string(record.Value), 10, 64)
	if err != nil {
		h.line.fetchFailed(h.member, fmt.Errorf("undecodable record at offset %d: %w", record.Offset, err))
		return
	}
	h.line.handled(h.member, seq)
}

// commit stores what was handled. A member the group has already removed is refused, and
// that refusal is part of what this experiment counts.
func (h handler) commit(ctx context.Context, handled []*kgo.Record) {
	if len(handled) == 0 {
		return
	}
	err := h.client.CommitRecords(ctx, handled...)
	switch {
	case err == nil:
		h.line.commitAccepted(h.member, nextOffsets(handled))
	case !errors.Is(err, context.Canceled):
		h.line.commitFailed(h.member, err)
	}
}

// nextOffsets is, per partition, the offset a commit of these records stores: one past the
// last of them.
func nextOffsets(records []*kgo.Record) map[int32]int64 {
	next := make(map[int32]int64)
	for _, record := range records {
		next[record.Partition] = max(next[record.Partition], record.Offset+1)
	}
	return next
}

// rewindTo is, per partition, the offset of the first record not handled.
func rewindTo(unhandled []*kgo.Record) map[string]map[int32]kgo.EpochOffset {
	first := make(map[int32]kgo.EpochOffset)
	for _, record := range unhandled {
		if _, seen := first[record.Partition]; !seen {
			first[record.Partition] = kgo.EpochOffset{Epoch: -1, Offset: record.Offset}
		}
	}
	return map[string]map[int32]kgo.EpochOffset{topic: first}
}

// allPartitions pauses what the member owns now and whatever it is assigned while paused.
func allPartitions() map[string][]int32 {
	all := make([]int32, 0, partitions)
	for p := range int32(partitions) {
		all = append(all, p)
	}
	return map[string][]int32{topic: all}
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
