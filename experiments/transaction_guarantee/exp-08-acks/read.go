package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

// count reads the topic to its end offsets through labkit, which refuses both a short read
// and a partition mid-election. This experiment's finding would be a shortfall, so an
// instrument able to under-count would be able to manufacture it.
func count(ctx context.Context, cfg *settings, out io.Writer) error {
	records, err := labkit.ReadAll(ctx, strings.Split(cfg.brokers, ","), cfg.topic, readStall)
	if err != nil {
		return err
	}

	return render(out, []string{
		fmt.Sprintf("readable now\t%d records", len(records)),
		fmt.Sprintf("lost\t%d of the %d that were acknowledged", max(cfg.records-len(records), 0), cfg.records),
	})
}

// leader prints which broker leads partition 0, so the script knows what to kill.
func leader(ctx context.Context, cfg *settings, out io.Writer) error {
	admin, closeAdmin, err := adminFor(cfg)
	if err != nil {
		return err
	}
	defer closeAdmin()

	part, err := labkit.Describe(ctx, admin, cfg.topic, 0)
	if err != nil {
		return err
	}
	return render(out, []string{
		fmt.Sprintf("LEADER_P0\t%d", part.Leader),
		fmt.Sprintf("in-sync replicas\t%v of %v", part.ISR, part.Replicas),
	})
}

func awaitShrunk(ctx context.Context, cfg *settings, out io.Writer) error {
	return awaitISR(ctx, cfg, out, "in-sync set shrank", labkit.Shrunk)
}

// awaitFull is how the script knows a restarted broker is ready, rather than guessing.
func awaitFull(ctx context.Context, cfg *settings, out io.Writer) error {
	return awaitISR(ctx, cfg, out, "in-sync set whole again", labkit.Whole)
}

func awaitISR(ctx context.Context, cfg *settings, out io.Writer, what string, reached func(isr, replicas int) bool) error {
	admin, closeAdmin, err := adminFor(cfg)
	if err != nil {
		return err
	}
	defer closeAdmin()

	waited, part, err := labkit.AwaitISR(ctx, admin, cfg.topic, 0, reached)
	if err != nil {
		return err
	}
	return render(out, []string{
		fmt.Sprintf("%s after\t%s", what, waited.Round(100*time.Millisecond)),
		fmt.Sprintf("in-sync replicas\t%v of %v", part.ISR, part.Replicas),
	})
}

func adminFor(cfg *settings) (*kadm.Client, func(), error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...))
	if err != nil {
		return nil, nil, fmt.Errorf("exp-08: kafka client: %w", err)
	}
	return kadm.NewClient(client), client.Close, nil
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
