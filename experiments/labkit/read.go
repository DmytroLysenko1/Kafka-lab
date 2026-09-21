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

// ReadAll returns every record of the topic from each partition's start to its end, in
// the order each partition delivered them. Silence for longer than stall before the end is
// an error, never an ending.
func ReadAll(ctx context.Context, brokers []string, topic string, stall time.Duration) ([]*kgo.Record, error) {
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("labkit: kafka client for offsets: %w", err)
	}
	extent, err := Spans(ctx, kadm.NewClient(admin), topic)
	admin.Close()
	if err != nil {
		return nil, err
	}

	reader, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("labkit: reader for %s: %w", topic, err)
	}
	defer reader.Close()

	return drain(ctx, reader, topic, newTracker(extent), stall)
}

func drain(ctx context.Context, reader *kgo.Client, topic string, progress *tracker, stall time.Duration) ([]*kgo.Record, error) {
	var records []*kgo.Record
	for !progress.done() {
		idle, cancel := context.WithTimeout(ctx, stall)
		fetches := reader.PollFetches(idle)
		cancel()

		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				return nil, fmt.Errorf("%w: %s, %d records short", ErrShortRead, topic, progress.remaining())
			}
			return nil, fmt.Errorf("labkit: read %s: %w", topic, err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, record)
			progress.saw(record.Partition, record.Offset)
		})
	}
	return records, nil
}

// tracker is the offset arithmetic behind "have we read everything", kept apart from the
// client so it can be tested without a cluster.
type tracker struct {
	extent map[int32]Span
	next   map[int32]int64
}

func newTracker(extent map[int32]Span) *tracker {
	next := make(map[int32]int64, len(extent))
	for partition, span := range extent {
		next[partition] = span.Start
	}
	return &tracker{extent: extent, next: next}
}

func (t *tracker) saw(partition int32, offset int64) {
	t.next[partition] = max(t.next[partition], offset+1)
}

func (t *tracker) done() bool {
	return t.remaining() == 0
}

func (t *tracker) remaining() int64 {
	var left int64
	for partition, span := range t.extent {
		left += max(span.End-t.next[partition], 0)
	}
	return left
}
