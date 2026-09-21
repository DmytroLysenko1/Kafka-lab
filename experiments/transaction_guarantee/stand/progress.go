package stand

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// progress answers "is there anything left to read" by comparing offsets, never by waiting
// for silence. A consumer that stops because nothing arrived for a while would report a
// short read as a finished one, and a short read here reads as lost payments — the
// instrument would manufacture the very finding the experiment is about.
type progress struct {
	end  map[int32]int64
	seen map[int32]int64
}

// newProgress seeds what has been read with the offsets the GROUP has committed, not with
// nothing. A consumer that replaces a killed one is never sent the records its predecessor
// already committed, so a tracker starting from zero would wait forever for a partition
// its predecessor finished — which is a hang, reported as a timeout, in the middle of an
// experiment about losing records.
func newProgress(end, committed map[int32]int64) *progress {
	seen := make(map[int32]int64, len(end))
	for partition, offset := range committed {
		seen[partition] = offset
	}
	return &progress{end: end, seen: seen}
}

// saw records that a record at this offset has been read.
func (p *progress) saw(partition int32, offset int64) {
	if next := offset + 1; next > p.seen[partition] {
		p.seen[partition] = next
	}
}

func (p *progress) reached() bool {
	for partition, end := range p.end {
		if p.seen[partition] < end {
			return false
		}
	}
	return true
}

// remaining is how many records the run has still to read, which makes a stuck consumer
// legible in an error rather than as a hang.
func (p *progress) remaining() int64 {
	var left int64
	for partition, end := range p.end {
		if behind := end - p.seen[partition]; behind > 0 {
			left += behind
		}
	}
	return left
}

func endOffsets(ctx context.Context, cfg *Settings, group string) (*progress, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.Brokers, ",")...))
	if err != nil {
		return nil, fmt.Errorf("%s: kafka client for end offsets: %w", cfg.Name, err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	listed, err := admin.ListEndOffsets(ctx, cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("%s: end offsets of %s: %w", cfg.Name, cfg.Topic, err)
	}

	end := make(map[int32]int64, len(listed[cfg.Topic]))
	for partition, offset := range listed[cfg.Topic] {
		end[partition] = offset.Offset
	}

	// A group that has never committed simply reports nothing, which is the first process
	// of a run starting from the beginning.
	stored, err := admin.FetchOffsets(ctx, group)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return nil, fmt.Errorf("%s: committed offsets of %s: %w", cfg.Name, group, err)
	}

	committed := make(map[int32]int64, len(end))
	stored.Each(func(offset kadm.OffsetResponse) {
		if offset.Topic == cfg.Topic {
			committed[offset.Partition] = offset.At
		}
	})
	return newProgress(end, committed), nil
}
