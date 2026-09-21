package labkit

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// GroupProgress is a Tracker for a consumer group that may be resuming: the topic's extent,
// with every partition already advanced to where the group committed. A group that has
// never committed simply reports nothing, which is the first process of a run starting
// from the beginning.
func GroupProgress(ctx context.Context, admin *kadm.Client, topic, group string) (*Tracker, error) {
	extent, err := Spans(ctx, admin, topic)
	if err != nil {
		return nil, err
	}
	progress := NewTracker(extent)

	committed, err := admin.FetchOffsets(ctx, group)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return nil, fmt.Errorf("labkit: committed offsets of %s: %w", group, err)
	}
	committed.Each(func(offset kadm.OffsetResponse) {
		if offset.Topic == topic && offset.Err == nil && offset.At >= 0 {
			progress.Seed(offset.Partition, offset.At)
		}
	})
	return progress, nil
}
