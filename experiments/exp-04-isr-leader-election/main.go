package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	topic          = "exp04.isr"

	pollEvery  = 250 * time.Millisecond
	maxRecords = 1_000_000
)

// phases run in this order, each one a separate process so the shell can kill and restart a
// broker between them and time the cluster's reaction from the failure itself.
var phases = []string{"baseline", "degraded", "recovered", "elected"}

var (
	errPhase   = errors.New("exp-04: -phase must be baseline, degraded, recovered or elected")
	errShape   = errors.New("exp-04: -records out of range")
	errNoShift = errors.New("exp-04: the cluster never reached the expected state inside the deadline")
)

type settings struct {
	brokers string
	phase   string
	records int
	since   int64
	timeout time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-04 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.phase, "phase", "", "baseline, degraded, recovered or elected")
	flag.IntVar(&cfg.records, "records", 300, "records to write in this phase")
	flag.Int64Var(&cfg.since, "since", 0, "unix milliseconds of the event this phase reacts to, for timing")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "deadline for this phase")
	flag.Parse()

	switch {
	case !slices.Contains(phases, cfg.phase):
		return errPhase
	case cfg.records <= 0 || cfg.records > maxRecords:
		return fmt.Errorf("%w: -records is %d", errShape, cfg.records)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	return measure(ctx, &cfg, os.Stdout)
}

func measure(ctx context.Context, cfg *settings, out io.Writer) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("exp-04: kafka client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	switch cfg.phase {
	case "baseline":
		return baseline(ctx, client, admin, cfg, out)
	case "degraded":
		return degraded(ctx, client, admin, cfg, out)
	case "recovered":
		return recovered(ctx, admin, cfg, out)
	default:
		return elected(ctx, admin, cfg, out)
	}
}

// baseline writes to a healthy cluster and records who leads what, so the later phases have
// something to be compared against.
func baseline(ctx context.Context, client *kgo.Client, admin *kadm.Client, cfg *settings, out io.Writer) error {
	written, err := write(ctx, client, cfg.records)
	if err != nil {
		return err
	}

	partitions, err := observe(ctx, admin)
	if err != nil {
		return err
	}

	state := inspect(partitions)
	lines := []string{
		"phase\tbaseline, every broker up",
		fmt.Sprintf("writes accepted\t%d of %d with acks=all", written, cfg.records),
		fmt.Sprintf("leaders by partition\t%v", leaders(partitions)),
		fmt.Sprintf("smallest ISR\t%d of 3 replicas", state.SmallestISR),
		fmt.Sprintf("under-replicated\t%d", state.UnderReplicated),
		// The script reads this line to learn which broker to kill: the one leading partition 0.
		fmt.Sprintf("LEADER_P0\t%d", leaders(partitions)[0]),
	}
	return render(out, lines)
}

// degraded waits for the cluster to notice the broker is gone, then asks the question that
// matters: with one replica missing and min.insync.replicas=2, can a payment still be written?
func degraded(ctx context.Context, client *kgo.Client, admin *kadm.Client, cfg *settings, out io.Writer) error {
	partitions, waited, err := await(ctx, admin, func(state health) bool {
		return state.UnderReplicated > 0
	})
	if err != nil {
		return err
	}

	written, writeErr := write(ctx, client, cfg.records)
	state := inspect(partitions)
	lines := []string{
		"phase\tdegraded, one broker killed",
		fmt.Sprintf("noticed after\t%s from the kill", since(cfg, waited)),
		fmt.Sprintf("leaders by partition\t%v", leaders(partitions)),
		fmt.Sprintf("under-replicated\t%d of %d", state.UnderReplicated, state.Partitions),
		fmt.Sprintf("smallest ISR\t%d, and min.insync.replicas is 2", state.SmallestISR),
		fmt.Sprintf("writes accepted\t%d of %d with acks=all%s", written, cfg.records, writeNote(writeErr)),
	}
	return render(out, lines)
}

// recovered waits for the replica to catch up again and then reports the part people expect
// to happen by itself and which does not: leadership staying where the failure moved it.
func recovered(ctx context.Context, admin *kadm.Client, cfg *settings, out io.Writer) error {
	partitions, waited, err := await(ctx, admin, func(state health) bool {
		return state.UnderReplicated == 0
	})
	if err != nil {
		return err
	}

	state := inspect(partitions)
	lines := []string{
		"phase\trecovered, the broker is back",
		fmt.Sprintf("ISR restored after\t%s from the restart", since(cfg, waited)),
		fmt.Sprintf("leaders by partition\t%v", leaders(partitions)),
		fmt.Sprintf("partitions led by a replacement\t%d of %d — auto leader rebalance is off, so leadership stays where the failure put it", state.OffPreferred, state.Partitions),
	}
	return render(out, lines)
}

// elected reports what an explicit preferred election did, which is the half of recovery
// nobody is told about: the replicas come back on their own, the leadership does not.
func elected(ctx context.Context, admin *kadm.Client, cfg *settings, out io.Writer) error {
	partitions, waited, err := await(ctx, admin, func(state health) bool {
		return state.OffPreferred == 0
	})
	if err != nil {
		return err
	}

	state := inspect(partitions)
	lines := []string{
		"phase\telected, preferred leadership asked for explicitly",
		fmt.Sprintf("leadership back after\t%s from the election", since(cfg, waited)),
		fmt.Sprintf("leaders by partition\t%v", leaders(partitions)),
		fmt.Sprintf("partitions led by a replacement\t%d of %d", state.OffPreferred, state.Partitions),
	}
	return render(out, lines)
}

func await(ctx context.Context, admin *kadm.Client, reached func(health) bool) ([]partition, time.Duration, error) {
	started := time.Now()

	for ctx.Err() == nil {
		partitions, err := observe(ctx, admin)
		if err == nil && reached(inspect(partitions)) {
			return partitions, time.Since(started), nil
		}

		select {
		case <-ctx.Done():
		case <-time.After(pollEvery):
		}
	}
	return nil, 0, errNoShift
}

func observe(ctx context.Context, admin *kadm.Client) ([]partition, error) {
	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("exp-04: describe %s: %w", topic, err)
	}

	partitions := make([]partition, 0, len(details[topic].Partitions))
	for _, detail := range details[topic].Partitions {
		partitions = append(partitions, partition{
			ID:       detail.Partition,
			Leader:   detail.Leader,
			Replicas: detail.Replicas,
			ISR:      detail.ISR,
		})
	}
	return partitions, nil
}

// write reports how many records the cluster accepted rather than failing the phase: a
// refusal is a result here, not an error.
func write(ctx context.Context, client *kgo.Client, records int) (int, error) {
	batch := make([]*kgo.Record, 0, records)
	for i := range records {
		batch = append(batch, &kgo.Record{
			Topic: topic,
			Key:   fmt.Appendf(nil, "pay-%05d", i),
			Value: fmt.Appendf(nil, `{"seq":%d}`, i),
		})
	}

	accepted := 0
	var failure error
	for _, result := range client.ProduceSync(ctx, batch...) {
		if result.Err != nil {
			failure = result.Err
			continue
		}
		accepted++
	}
	return accepted, failure
}

func writeNote(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf(" — refused: %v", err)
}

func since(cfg *settings, waited time.Duration) time.Duration {
	if cfg.since <= 0 {
		return waited.Round(100 * time.Millisecond)
	}
	return time.Since(time.UnixMilli(cfg.since)).Round(100 * time.Millisecond)
}

func render(out io.Writer, lines []string) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("exp-04: write report: %w", err)
		}
	}
	return table.Flush()
}
