package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// count reads the topic from wherever it starts to its last offset and reports what is
// actually there. Reading to the end offset rather than stopping on silence matters more
// here than anywhere: this experiment's finding IS a shortfall, so an instrument that can
// under-count would manufacture it.
func count(ctx context.Context, cfg *settings, out io.Writer) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ConsumeTopics(cfg.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("exp-08: kafka client: %w", err)
	}
	defer client.Close()

	start, end, err := bounds(ctx, cfg)
	if err != nil {
		return err
	}

	readable := 0
	for last := start - 1; last < end-1; {
		idle, cancel := context.WithTimeout(ctx, readStall)
		fetches := client.PollRecords(idle, pollBatch)
		cancel()

		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				return fmt.Errorf("%w: %s, wanted up to %d", errShortRead, cfg.topic, end-1)
			}
			return fmt.Errorf("exp-08: read %s: %w", cfg.topic, err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			readable++
			last = max(last, record.Offset)
		})
	}

	return render(out, []string{
		fmt.Sprintf("readable now\t%d records", readable),
		fmt.Sprintf("lost\t%d of the %d that were acknowledged", max(cfg.records-readable, 0), cfg.records),
		fmt.Sprintf("log\tstarts at %d, ends at %d", start, end),
	})
}

// bounds reports where the log begins and ends, and refuses to answer while the partition
// is mid-election. A leader that has just been replaced reports -1 for its offsets, and an
// instrument that passed that on would print "0 records readable" from a read loop that
// never ran — manufacturing exactly the finding this experiment claims to have measured.
func bounds(ctx context.Context, cfg *settings) (int64, int64, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...))
	if err != nil {
		return 0, 0, fmt.Errorf("exp-08: kafka client for offsets: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	var last error
	for ctx.Err() == nil {
		start, end, err := offsets(ctx, admin, cfg.topic)
		if err == nil && start >= 0 && end >= start {
			return start, end, nil
		}
		last = err

		select {
		case <-ctx.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
	if last != nil {
		return 0, 0, fmt.Errorf("%w: %s: %w", errNoOffsets, cfg.topic, last)
	}
	return 0, 0, fmt.Errorf("%w: %s reported an end offset before its start", errNoOffsets, cfg.topic)
}

func offsets(ctx context.Context, admin *kadm.Client, topic string) (int64, int64, error) {
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("exp-08: start offsets of %s: %w", topic, err)
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("exp-08: end offsets of %s: %w", topic, err)
	}

	// Lookup reports whether the partition was in the response at all, and the response
	// itself carries a per-partition error — a leader mid-election answers with one rather
	// than with an offset. Both are checked, because an unnoticed error here reads as an
	// empty log, which is this experiment's finding.
	start, ok := starts.Lookup(topic, 0)
	if !ok {
		return 0, 0, fmt.Errorf("exp-08: %s reported no partition 0 start offset", topic)
	}
	if start.Err != nil {
		return 0, 0, fmt.Errorf("exp-08: start offset of %s: %w", topic, start.Err)
	}

	end, ok := ends.Lookup(topic, 0)
	if !ok {
		return 0, 0, fmt.Errorf("exp-08: %s reported no partition 0 end offset", topic)
	}
	if end.Err != nil {
		return 0, 0, fmt.Errorf("exp-08: end offset of %s: %w", topic, end.Err)
	}
	return start.Offset, end.Offset, nil
}

// leader prints which broker leads partition 0, so the script knows what to kill.
func leader(ctx context.Context, cfg *settings, out io.Writer) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...))
	if err != nil {
		return fmt.Errorf("exp-08: kafka client: %w", err)
	}
	defer client.Close()

	details, err := kadm.NewClient(client).ListTopics(ctx, cfg.topic)
	if err != nil {
		return fmt.Errorf("exp-08: describe %s: %w", cfg.topic, err)
	}

	part, ok := details[cfg.topic].Partitions[0]
	if !ok {
		return fmt.Errorf("exp-08: %s has no partition 0", cfg.topic)
	}
	return render(out, []string{
		fmt.Sprintf("LEADER_P0\t%d", part.Leader),
		fmt.Sprintf("in-sync replicas\t%v of %v", part.ISR, part.Replicas),
	})
}

// awaitFull waits until every replica is back in the in-sync set, which is how the script
// knows a restarted broker is ready rather than guessing with a sleep.
func awaitFull(ctx context.Context, cfg *settings, out io.Writer) error {
	return awaitISR(ctx, cfg, out, "in-sync set whole again", func(isr, replicas int) bool { return isr == replicas })
}

// awaitShrunk waits until the cluster has noticed the broker is gone, rather than the
// script sleeping for a guessed while: how long fencing takes is exp-04's measurement, and
// borrowing a number from it here would make this run depend on that one staying true.
func awaitShrunk(ctx context.Context, cfg *settings, out io.Writer) error {
	return awaitISR(ctx, cfg, out, "in-sync set shrank", func(isr, replicas int) bool { return isr < replicas })
}

func awaitISR(ctx context.Context, cfg *settings, out io.Writer, what string, reached func(isr, replicas int) bool) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...))
	if err != nil {
		return fmt.Errorf("exp-08: kafka client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	started := time.Now()
	for ctx.Err() == nil {
		details, err := admin.ListTopics(ctx, cfg.topic)
		if err == nil {
			if part, ok := details[cfg.topic].Partitions[0]; ok && reached(len(part.ISR), len(part.Replicas)) {
				return render(out, []string{
					fmt.Sprintf("%s after\t%s", what, time.Since(started).Round(100*time.Millisecond)),
					fmt.Sprintf("in-sync replicas\t%v of %v", part.ISR, part.Replicas),
				})
			}
		}

		select {
		case <-ctx.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
	return fmt.Errorf("%w on %s", errNoShrink, cfg.topic)
}

func render(out io.Writer, lines []string) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("exp-08: write report: %w", err)
		}
	}
	return table.Flush()
}
