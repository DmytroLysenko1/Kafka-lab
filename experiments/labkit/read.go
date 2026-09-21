// Package labkit holds the one piece of instrument every experiment that reads a log back
// needs, and that four of them wrote separately before it moved here: reading a topic to
// its end offsets, and refusing to report a short read as a finished one.
package labkit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrNoOffsets = errors.New("labkit: the topic never reported usable offsets")
	ErrShortRead = errors.New("labkit: the log went quiet before its last offset")
)

// Span is the part of one partition's log that exists right now.
type Span struct {
	Start int64
	End   int64
}

// Spans reports where each partition begins and ends, and refuses to answer while any of
// them is mid-election. A leader that has just been replaced answers offset requests with
// OFFSET_NOT_AVAILABLE and -1, and a reader that took -1 at face value would run its read
// loop zero times and report an empty log — which is exactly the kind of finding the
// experiments that use this are trying to measure, manufactured by the instrument instead.
func Spans(ctx context.Context, admin *kadm.Client, topic string) (map[int32]Span, error) {
	var last error
	for ctx.Err() == nil {
		spans, err := spans(ctx, admin, topic)
		if err == nil {
			return spans, nil
		}
		last = err

		select {
		case <-ctx.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("%w: %s: %w", ErrNoOffsets, topic, errors.Join(last, ctx.Err()))
}

func spans(ctx context.Context, admin *kadm.Client, topic string) (map[int32]Span, error) {
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("labkit: start offsets of %s: %w", topic, err)
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("labkit: end offsets of %s: %w", topic, err)
	}

	result := make(map[int32]Span, len(ends[topic]))
	for partition, end := range ends[topic] {
		start, ok := starts.Lookup(topic, partition)
		switch {
		case !ok:
			return nil, fmt.Errorf("labkit: %s partition %d has no start offset", topic, partition)
		case start.Err != nil:
			return nil, fmt.Errorf("labkit: start offset of %s partition %d: %w", topic, partition, start.Err)
		case end.Err != nil:
			return nil, fmt.Errorf("labkit: end offset of %s partition %d: %w", topic, partition, end.Err)
		case start.Offset < 0 || end.Offset < start.Offset:
			return nil, fmt.Errorf("labkit: %s partition %d reported %d..%d", topic, partition, start.Offset, end.Offset)
		}
		result[partition] = Span{Start: start.Offset, End: end.Offset}
	}
	return result, nil
}

// ReadAll returns every data record of the topic from each partition's start to its end,
// in the order each partition delivered them. Silence for longer than stall before the end
// is an error, never an ending. opts reach the reader: an isolation level, most often.
//
// Control records — the commit and abort markers of transactions — are kept and tracked
// but never returned. On a transactional topic the last offset of a partition is a marker,
// and a read_committed reader is never handed it, so a reader waiting to see that offset
// would wait forever and report the silence as a short read.
func ReadAll(ctx context.Context, brokers []string, topic string, stall time.Duration, opts ...kgo.Opt) ([]*kgo.Record, error) {
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("labkit: kafka client for offsets: %w", err)
	}
	extent, err := Spans(ctx, kadm.NewClient(admin), topic)
	admin.Close()
	if err != nil {
		return nil, err
	}

	reader, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.KeepControlRecords(),
	}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("labkit: reader for %s: %w", topic, err)
	}
	defer reader.Close()

	return drain(ctx, reader, topic, NewTracker(extent), stall)
}

func drain(ctx context.Context, reader *kgo.Client, topic string, progress *Tracker, stall time.Duration) ([]*kgo.Record, error) {
	var records []*kgo.Record
	for !progress.Done() {
		idle, cancel := context.WithTimeout(ctx, stall)
		fetches := reader.PollFetches(idle)
		cancel()

		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				return nil, fmt.Errorf("%w: %s, %d records short", ErrShortRead, topic, progress.Remaining())
			}
			return nil, fmt.Errorf("labkit: read %s: %w", topic, err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			progress.Saw(record.Partition, record.Offset)
			if !record.Attrs.IsControl() {
				records = append(records, record)
			}
		})
	}
	return records, nil
}

// Tracker is the offset arithmetic behind "have we read everything", kept apart from the
// client so it can be tested without a cluster.
type Tracker struct {
	extent map[int32]Span
	next   map[int32]int64
}

func NewTracker(extent map[int32]Span) *Tracker {
	next := make(map[int32]int64, len(extent))
	for partition, span := range extent {
		next[partition] = span.Start
	}
	return &Tracker{extent: extent, next: next}
}

// Seed records that a partition has already been read up to next — by a consumer group
// that committed it before this process existed. A consumer replacing a killed one is
// never sent what its predecessor committed, so without this a partition the predecessor
// finished would hold the run open forever: a hang, reported as a timeout, in the middle
// of an experiment about losing records.
func (t *Tracker) Seed(partition int32, next int64) {
	t.next[partition] = max(t.next[partition], next)
}

func (t *Tracker) Saw(partition int32, offset int64) {
	t.Seed(partition, offset+1)
}

func (t *Tracker) Done() bool {
	return t.Remaining() == 0
}

func (t *Tracker) Remaining() int64 {
	var left int64
	for partition, span := range t.extent {
		left += max(span.End-t.next[partition], 0)
	}
	return left
}
