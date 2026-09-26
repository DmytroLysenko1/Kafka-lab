package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

// partitionReadStall is how long the read of the main topic may go without a record before
// it counts as stuck; the topic holds one cell's few hundred payments.
const partitionReadStall = 10 * time.Second

var errNoEventID = errors.New("exp-18: a record on the main topic has no event_id header")

// partitionsOf reads where each payment landed. Payments are keyed by payment id, so the
// locked merchant's are spread over every partition, and order only exists within one:
// comparing across partitions would count reordering the run had without any chain.
// Read off the topic rather than recomputed from the key, so that it stays true whatever
// partitioner the publisher uses.
func partitionsOf(ctx context.Context, topic string) (map[int64]int32, error) {
	records, err := labkit.ReadAll(ctx, brokers(), topic, partitionReadStall)
	if err != nil {
		return nil, fmt.Errorf("read %s for partitions: %w", topic, err)
	}
	partitions := make(map[int64]int32, len(records))
	for _, record := range records {
		eventID, err := eventIDOf(record.Headers)
		if err != nil {
			return nil, fmt.Errorf("%s partition %d offset %d: %w", topic, record.Partition, record.Offset, err)
		}
		partitions[eventID] = record.Partition
	}
	return partitions, nil
}

func eventIDOf(headers []kgo.RecordHeader) (int64, error) {
	index := slices.IndexFunc(headers, func(header kgo.RecordHeader) bool { return header.Key == "event_id" })
	if index < 0 {
		return 0, errNoEventID
	}
	eventID, err := strconv.ParseInt(string(headers[index].Value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("event_id %q: %w", headers[index].Value, err)
	}
	return eventID, nil
}
