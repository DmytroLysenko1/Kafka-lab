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
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	topic          = "exp04.isr"

	pollEvery  = 250 * time.Millisecond
	maxRecords = 1_000_000
)

// phases run in this order, each one a separate process so the shell can kill and restart a
// broker between them and time the cluster's reaction from the failure itself.
var phases = []string{"baseline", "degraded", "recovered", "elected", "readback"}

var (
	errPhase    = errors.New("exp-04: -phase must be baseline, degraded, recovered, elected or readback")
	errShape    = errors.New("exp-04: -records out of range")
	errNoShift  = errors.New("exp-04: the cluster never reached the expected state inside the deadline")
	errNoConfig = errors.New("exp-04: the topic does not report the config the phase needs")
)

type settings struct {
	brokers string
	phase   string
	records int
	expect  int
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
	flag.IntVar(&cfg.expect, "expect", 0, "readback: how many writes the earlier phases were told were accepted")
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
	case "readback":
		return readback(ctx, cfg, out)
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
		fmt.Sprintf("smallest ISR\t%d of %d replicas", state.SmallestISR, state.Replicas),
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
		return state.UnderReplicated > 0 && state.Leaderless == 0
	})
	if err != nil {
		return err
	}
	timing := reached(cfg, time.Now(), "noticed", "the kill", waited)

	insync, err := minInsyncReplicas(ctx, admin)
	if err != nil {
		return err
	}

	written, writeErr := write(ctx, client, cfg.records)
	state := inspect(partitions)
	return render(out, slices.Concat(
		[]string{"phase\tdegraded, one broker killed"},
		timing,
		[]string{
			fmt.Sprintf("leaders by partition\t%v", leaders(partitions)),
			fmt.Sprintf("leaderless partitions\t%d", state.Leaderless),
			fmt.Sprintf("under-replicated\t%d of %d", state.UnderReplicated, state.Partitions),
			fmt.Sprintf("smallest ISR\t%d, with min.insync.replicas %d", state.SmallestISR, insync),
			fmt.Sprintf("writes accepted\t%d of %d with acks=all%s", written, cfg.records, writeNote(writeErr)),
		},
	))
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
	timing := reached(cfg, time.Now(), "ISR restored", "the restart", waited)

	state := inspect(partitions)
	return render(out, slices.Concat(
		[]string{"phase\trecovered, the broker is back"},
		timing,
		[]string{
			fmt.Sprintf("leaders by partition\t%v", leaders(partitions)),
			fmt.Sprintf("smallest ISR\t%d of %d replicas", state.SmallestISR, state.Replicas),
			fmt.Sprintf("partitions led by a replacement\t%d of %d", state.OffPreferred, state.Partitions),
		},
	))
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
	timing := reached(cfg, time.Now(), "leadership back", "the election", waited)

	state := inspect(partitions)
	return render(out, slices.Concat(
		[]string{"phase\telected, preferred leadership asked for explicitly"},
		timing,
		[]string{
			fmt.Sprintf("leaders by partition\t%v", leaders(partitions)),
			fmt.Sprintf("smallest ISR\t%d of %d replicas", state.SmallestISR, state.Replicas),
			fmt.Sprintf("partitions led by a replacement\t%d of %d", state.OffPreferred, state.Partitions),
		},
	))
}

func await(ctx context.Context, admin *kadm.Client, reached func(health) bool) ([]partition, time.Duration, error) {
	started := time.Now()

	// The last observation error is carried to the deadline: without it a topic that is
	// missing, or a cluster that cannot be reached at all, is reported as the cluster
	// failing to change — an instrument fault described as a finding.
	var last error
	for ctx.Err() == nil {
		partitions, err := observe(ctx, admin)
		switch {
		case err != nil:
			last = err
		case reached(inspect(partitions)):
			return partitions, time.Since(started), nil
		}

		select {
		case <-ctx.Done():
		case <-time.After(pollEvery):
		}
	}
	if last != nil {
		return nil, 0, fmt.Errorf("%w: last observation failed: %w", errNoShift, last)
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

// readbackStall is how long the log may go quiet before the read is called short. A short
// read is a failed run, never "the rest was lost": that would be the claim this phase
// exists to test, produced by the instrument instead of by the cluster.
const readbackStall = 10 * time.Second

// readback reads the whole partition after the leader was killed and replaced, and counts
// it against every write the cluster acknowledged: "a pause, not data" is a claim about
// the log, so the log is what answers it.
func readback(ctx context.Context, cfg *settings, out io.Writer) error {
	records, err := labkit.ReadAll(ctx, strings.Split(cfg.brokers, ","), topic, readbackStall)
	if err != nil {
		return fmt.Errorf("exp-04: read the topic back: %w", err)
	}
	return render(out, []string{
		"phase	readback, after the failover and the recovery",
		fmt.Sprintf("acknowledged	%d writes, over every phase", cfg.expect),
		fmt.Sprintf("readable	%d records", len(records)),
		fmt.Sprintf("lost	%d", max(cfg.expect-len(records), 0)),
	})
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

// reached reports how long a phase waited, as two numbers that mean different things.
// The first is measured from the event the shell timed and is an UPPER BOUND: the cluster
// may have got there while this process was still starting. The second is how long this
// process actually spent polling — near zero means the cluster was already in the wanted
// state at the first look, so the bound above is process startup and nothing else.
func reached(cfg *settings, now time.Time, what, event string, waited time.Duration) []string {
	return []string{
		fmt.Sprintf("%s within\t%s of %s", what, since(cfg, now, waited), event),
		fmt.Sprintf("  of that, spent polling\t%s", waited.Round(100*time.Millisecond)),
	}
}

// minInsyncReplicas asks the cluster rather than trusting the YAML: the number this
// experiment is about is the one the broker is enforcing, not the one we meant to set.
func minInsyncReplicas(ctx context.Context, admin *kadm.Client) (int, error) {
	resources, err := admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("exp-04: describe config of %s: %w", topic, err)
	}
	resource, err := resources.On(topic, nil)
	if err != nil {
		return 0, fmt.Errorf("exp-04: config of %s: %w", topic, err)
	}

	for _, config := range resource.Configs {
		if config.Key != "min.insync.replicas" {
			continue
		}
		value, err := strconv.Atoi(config.MaybeValue())
		if err != nil {
			return 0, fmt.Errorf("exp-04: min.insync.replicas of %s reads %q: %w", topic, config.MaybeValue(), err)
		}
		return value, nil
	}
	return 0, fmt.Errorf("%w: min.insync.replicas on %s", errNoConfig, topic)
}

// since takes now as an argument rather than reading the clock: the two numbers this
// experiment publishes are elapsed times, and a function that cannot be given a clock
// cannot be tested for what it reports.
func since(cfg *settings, now time.Time, waited time.Duration) time.Duration {
	elapsed := now.Sub(time.UnixMilli(cfg.since))
	// The shell's timestamp and this process's clock are two wall clocks; an NTP step
	// between them can make the bound come out shorter than the polling it contains, or
	// negative. Fall back to what this process timed itself, which is monotonic.
	if cfg.since <= 0 || elapsed < waited {
		return waited.Round(100 * time.Millisecond)
	}
	return elapsed.Round(100 * time.Millisecond)
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
