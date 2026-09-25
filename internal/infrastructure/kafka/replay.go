package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrReplay        = errors.New("kafka: replay dead letters")
	ErrReplayStalled = errors.New("kafka: the dead letter topic went quiet before its end was reached")
)

// replayIdle is how long a poll may return nothing before the replay gives up. Silence
// while records are known to remain is a broken read, never the end of the topic.
const replayIdle = 10 * time.Second

type replays interface {
	Replay(ctx context.Context, record *kgo.Record, topic string) error
}

// ReplayRequest is one class of dead letters to send back into a chain. Each class keeps
// its own group, so replaying one class never moves past letters of another: those stay
// where they are for the replay that is meant for them.
type ReplayRequest struct {
	DeadLetterTopic string
	Class           Class
	Into            string
	Group           string
}

type ReplayReport struct {
	Replayed int
	Skipped  int
}

// Replay sends every dead letter of the requested class that was in the topic when it
// started into the first tier, and stops there: letters that arrive during the replay are
// the next replay's business. It commits as it goes, so a replay that fails halfway picks
// up after the last letter it had actually sent.
func Replay(ctx context.Context, brokers []string, request ReplayRequest, sink replays) (ReplayReport, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(request.Group),
		kgo.ConsumeTopics(request.DeadLetterTopic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return ReplayReport{}, fmt.Errorf("%w: %w", ErrReplay, err)
	}
	defer client.Close()

	remaining, err := unreplayed(ctx, kadm.NewClient(client), request)
	if err != nil {
		return ReplayReport{}, err
	}

	run := replayRun{client: client, request: request, sink: sink, remaining: remaining}
	return run.untilEnd(ctx)
}

// unreplayed is, per partition, the range still to read: from where the group stands, or
// from the start of the log if it has never stood anywhere, up to the end as of now.
func unreplayed(ctx context.Context, admin *kadm.Client, request ReplayRequest) (map[int32]offsetRange, error) {
	starts, err := listed(admin.ListStartOffsets(ctx, request.DeadLetterTopic))
	if err != nil {
		return nil, fmt.Errorf("%w: start offsets of %s: %w", ErrReplay, request.DeadLetterTopic, err)
	}
	ends, err := listed(admin.ListEndOffsets(ctx, request.DeadLetterTopic))
	if err != nil {
		return nil, fmt.Errorf("%w: end offsets of %s: %w", ErrReplay, request.DeadLetterTopic, err)
	}
	committed, err := admin.FetchOffsetsForTopics(ctx, request.Group, request.DeadLetterTopic)
	if err == nil {
		err = committed.Error()
	}
	if err != nil {
		return nil, fmt.Errorf("%w: offsets of group %s: %w", ErrReplay, request.Group, err)
	}

	remaining := make(map[int32]offsetRange)
	ends.Each(func(end kadm.ListedOffset) {
		position := startingPoint(starts, committed, end)
		if position < end.Offset {
			remaining[end.Partition] = offsetRange{next: position, end: end.Offset}
		}
	})
	return remaining, nil
}

// listed folds the per-partition errors of a listing into its own: a partition that failed
// to answer carries offset -1, and summed unchecked it reads as an empty partition.
func listed(offsets kadm.ListedOffsets, err error) (kadm.ListedOffsets, error) {
	if err != nil {
		return nil, err
	}
	return offsets, offsets.Error()
}

func startingPoint(starts kadm.ListedOffsets, committed kadm.OffsetResponses, end kadm.ListedOffset) int64 {
	position := int64(0)
	if from, ok := starts.Lookup(end.Topic, end.Partition); ok {
		position = from.Offset
	}
	if at, ok := committed.Lookup(end.Topic, end.Partition); ok {
		position = max(position, at.At)
	}
	return position
}

type offsetRange struct {
	next int64
	end  int64
}

type replayRun struct {
	client    *kgo.Client
	request   ReplayRequest
	sink      replays
	remaining map[int32]offsetRange
	report    ReplayReport
}

func (r *replayRun) untilEnd(ctx context.Context) (ReplayReport, error) {
	for len(r.remaining) > 0 {
		if err := r.pollOnce(ctx); err != nil {
			return r.report, err
		}
	}
	return r.report, nil
}

func (r *replayRun) pollOnce(ctx context.Context) error {
	polling, cancel := context.WithTimeout(ctx, replayIdle)
	defer cancel()

	fetches := r.client.PollRecords(polling, maxRecordsPerPoll)
	defer r.client.AllowRebalance()

	if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrReplay, err)
	}
	if fetches.NumRecords() == 0 {
		return fmt.Errorf("%w: %d partitions unfinished", ErrReplayStalled, len(r.remaining))
	}

	sent, failure := r.sendAll(ctx, fetches.Records())
	if err := r.commit(ctx, sent); err != nil {
		return err
	}
	return failure
}

// sendAll replays the letters inside the range taken at the start and reports which ones
// it is done with, sent or deliberately skipped: both may be committed.
func (r *replayRun) sendAll(ctx context.Context, records []*kgo.Record) ([]*kgo.Record, error) {
	done := make([]*kgo.Record, 0, len(records))
	for _, record := range records {
		within, ok := r.remaining[record.Partition]
		if !ok || record.Offset >= within.end {
			continue
		}
		if err := r.send(ctx, record); err != nil {
			return done, err
		}
		done = append(done, record)
		r.advance(record)
	}
	return done, nil
}

func (r *replayRun) send(ctx context.Context, record *kgo.Record) error {
	if Class(headerValue(record.Headers, headerDeadLetterClass)) != r.request.Class {
		r.report.Skipped++
		return nil
	}
	if err := r.sink.Replay(ctx, record, r.request.Into); err != nil {
		return fmt.Errorf("%w: offset %d of partition %d: %w", ErrReplay, record.Offset, record.Partition, err)
	}
	r.report.Replayed++
	return nil
}

func (r *replayRun) advance(record *kgo.Record) {
	within := r.remaining[record.Partition]
	within.next = record.Offset + 1
	if within.next >= within.end {
		delete(r.remaining, record.Partition)
		return
	}
	r.remaining[record.Partition] = within
}

func (r *replayRun) commit(ctx context.Context, done []*kgo.Record) error {
	if len(done) == 0 {
		return nil
	}
	committing, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()

	if err := r.client.CommitRecords(committing, done...); err != nil {
		return fmt.Errorf("%w: commit %d letters: %w", ErrReplay, len(done), err)
	}
	return nil
}
