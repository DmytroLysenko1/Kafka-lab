package labkit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

var ErrNeverReached = errors.New("labkit: the in-sync set never reached the wanted state inside the deadline")

// Describe reports one partition as the cluster sees it right now.
func Describe(ctx context.Context, admin *kadm.Client, topic string, partition int32) (kadm.PartitionDetail, error) {
	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return kadm.PartitionDetail{}, fmt.Errorf("labkit: describe %s: %w", topic, err)
	}
	part, ok := details[topic].Partitions[partition]
	if !ok {
		return kadm.PartitionDetail{}, fmt.Errorf("labkit: %s has no partition %d", topic, partition)
	}
	return part, nil
}

// AwaitISR polls until the partition's in-sync set satisfies reached, and reports how long
// that took. It exists so no experiment sleeps for a guessed while: how long the cluster
// takes to notice a broker is exp-04's measurement, and borrowing a number from it would
// make every other run depend on that number staying true.
func AwaitISR(ctx context.Context, admin *kadm.Client, topic string, partition int32, reached func(isr, replicas int) bool) (time.Duration, kadm.PartitionDetail, error) {
	started := time.Now()

	var last error
	for ctx.Err() == nil {
		part, err := Describe(ctx, admin, topic, partition)
		switch {
		case err != nil:
			last = err
		case reached(len(part.ISR), len(part.Replicas)):
			return time.Since(started), part, nil
		}

		select {
		case <-ctx.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
	return 0, kadm.PartitionDetail{}, fmt.Errorf("%w: %s: %w", ErrNeverReached, topic, errors.Join(last, ctx.Err()))
}

// Shrunk and Whole are the two states experiments wait for.
func Shrunk(isr, replicas int) bool { return isr < replicas }
func Whole(isr, replicas int) bool  { return isr == replicas }
