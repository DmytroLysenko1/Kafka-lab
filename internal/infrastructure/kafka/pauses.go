package kafka

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// pauseSchedule is, per paused partition, when the record it was stopped at comes due. It
// belongs to one poll loop, which is its only reader and writer.
type pauseSchedule map[int32]time.Time

func (p pauseSchedule) hold(partition int32, until time.Time) { p[partition] = until }

// takeDue removes and returns every partition whose record is due by now.
func (p pauseSchedule) takeDue(now time.Time) []int32 {
	var due []int32
	maps.DeleteFunc(p, func(partition int32, at time.Time) bool {
		if at.After(now) {
			return false
		}
		due = append(due, partition)
		return true
	})
	slices.Sort(due)
	return due
}

// next is the earliest time a paused partition comes due, if any is paused.
func (p pauseSchedule) next() (time.Time, bool) {
	if len(p) == 0 {
		return time.Time{}, false
	}
	return slices.MinFunc(slices.Collect(maps.Values(p)), time.Time.Compare), true
}

// untilNextDue bounds a poll by the earliest paused partition's due time. With nothing
// paused it is the caller's context unchanged.
func (c *Consumer) untilNextDue(ctx context.Context) (context.Context, context.CancelFunc) {
	due, paused := c.paused.next()
	if !paused {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, due)
}

// pausedPartitionCameDue tells a poll that ended because a paused partition is due from
// one that ended because the consumer is stopping or the fetch failed. It judges by the
// error the poll returned, not by the poll's context read afterwards: that context may
// expire a moment after the poll came back with records and a real error, and those
// records would be dropped as if nothing had arrived.
func pausedPartitionCameDue(ctx context.Context, fetches kgo.Fetches, err error) bool {
	return ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) && fetches.NumRecords() == 0
}

// resumeDue starts fetching again every partition whose record is now due.
func (c *Consumer) resumeDue(now time.Time) {
	if due := c.paused.takeDue(now); len(due) > 0 {
		c.client.ResumeFetchPartitions(map[string][]int32{c.stage.Topic: due})
	}
}

// holdUntilDue moves each waiting partition back to its first unhandled record, and stops
// fetching the ones whose record is not due yet — those alone: a partition that has waited
// long enough, or one full of records that are due, must not sit behind another's delay.
// A partition stopped by the batch budget is only rewound; it has nothing to wait for.
// Both happen before the rebalance is released: franz-go documents SetOffsets as safe only
// while no revoke can run, and BlockRebalanceOnPoll is what holds revokes off until
// AllowRebalance. The waiting itself happens outside any batch, holding nothing — the shape
// exp-16 measured against waiting inside the batch.
func (c *Consumer) holdUntilDue(waiting map[int32]*kgo.Record) {
	if len(waiting) == 0 {
		return
	}

	now := time.Now()
	rewind := make(map[int32]kgo.EpochOffset, len(waiting))
	notDue := make([]int32, 0, len(waiting))
	for partition, record := range waiting {
		rewind[partition] = kgo.EpochOffset{Epoch: -1, Offset: record.Offset}
		if wait := c.stage.untilDue(record, now); wait > 0 {
			c.paused.hold(partition, now.Add(wait))
			notDue = append(notDue, partition)
		}
	}

	c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{c.stage.Topic: rewind})
	if len(notDue) > 0 {
		c.client.PauseFetchPartitions(map[string][]int32{c.stage.Topic: notDue})
	}
}
