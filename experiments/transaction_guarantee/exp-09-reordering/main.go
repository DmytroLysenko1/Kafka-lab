package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	topic          = "exp09.payments"

	readStall   = 15 * time.Second
	tick        = 5 * time.Millisecond
	maxDuration = 10 * time.Minute
	maxPerTick  = 1000
)

const (
	producerPlain      = "plain"
	producerIdempotent = "idempotent"
)

var (
	producers = []string{producerPlain, producerIdempotent}
	phases    = []string{"produce", "verify", "leader", "await-shrunk", "await-full"}
)

var (
	errPhase    = errors.New("exp-09: -phase must be produce, verify, leader, await-shrunk or await-full")
	errProducer = errors.New("exp-09: -producer must be plain or idempotent")
	errShape    = errors.New("exp-09: -duration or -per-tick out of range")
	errDecode   = errors.New("exp-09: undecodable record")
)

type settings struct {
	brokers  string
	phase    string
	producer string
	duration time.Duration
	perTick  int
	produced int
	timeout  time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-09 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.phase, "phase", "", strings.Join(phases, ", "))
	flag.StringVar(&cfg.producer, "producer", producerPlain, strings.Join(producers, " or "))
	flag.DurationVar(&cfg.duration, "duration", 90*time.Second, "how long the producer keeps producing")
	flag.IntVar(&cfg.perTick, "per-tick", 10, "payments started every 5 ms")
	flag.IntVar(&cfg.produced, "produced", 0, "events the produce phase reported, for verify")
	flag.DurationVar(&cfg.timeout, "timeout", 5*time.Minute, "deadline for this phase")
	flag.Parse()

	switch {
	case !slices.Contains(phases, cfg.phase):
		return errPhase
	case !slices.Contains(producers, cfg.producer):
		return errProducer
	case cfg.duration <= 0 || cfg.duration > maxDuration || cfg.perTick <= 0 || cfg.perTick > maxPerTick:
		return fmt.Errorf("%w: -duration %s, -per-tick %d", errShape, cfg.duration, cfg.perTick)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	return measure(ctx, &cfg, os.Stdout)
}

func measure(ctx context.Context, cfg *settings, out io.Writer) error {
	switch cfg.phase {
	case "produce":
		return produce(ctx, cfg, out)
	case "verify":
		return verify(ctx, cfg, out)
	case "leader":
		return leader(ctx, cfg, out)
	case "await-shrunk":
		return await(ctx, cfg, out, "in-sync set shrank", labkit.Shrunk)
	default:
		return await(ctx, cfg, out, "in-sync set whole again", labkit.Whole)
	}
}

// producerOptions is the whole difference between the two runs. Both ask for acks=all,
// so a record is only acknowledged once the in-sync set has it; what differs is whether
// the broker can tell a retried batch from a new one.
func producerOptions(cfg *settings) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Small, frequent requests, so several are in flight to the leader when one of
		// them fails: reordering needs an earlier request to fail while a later one lands.
		kgo.ProducerLinger(0),
		kgo.ProducerBatchMaxBytes(16 << 10),
		// Long enough to outlast the refusals while a follower is frozen: a record given
		// up on would read as missing and hide the reordering behind a loss.
		kgo.RecordDeliveryTimeout(4 * time.Minute),
	}
	if cfg.producer == producerPlain {
		// franz-go defaults to one request in flight without idempotence, unlike Java's
		// five; five is set here so the comparison is with the configuration people
		// actually run when they turn idempotence off.
		opts = append(opts, kgo.DisableIdempotentWrite(), kgo.MaxProduceRequestsInflightPerBroker(5))
	}
	return opts
}

// produce starts a steady stream of payments, each a capture followed by its refund, and
// keeps it going while the script freezes and thaws a follower.
func produce(ctx context.Context, cfg *settings, out io.Writer) error {
	client, err := kgo.NewClient(producerOptions(cfg)...)
	if err != nil {
		return fmt.Errorf("exp-09: kafka client: %w", err)
	}
	defer client.Close()

	var acked, failed atomic.Int64
	var lastFailure atomic.Value
	promise := func(_ *kgo.Record, err error) {
		if err != nil {
			failed.Add(1)
			lastFailure.Store(err.Error())
			return
		}
		acked.Add(1)
	}

	sent, err := stream(ctx, client, cfg, promise)
	if err != nil {
		return err
	}
	if err := client.Flush(ctx); err != nil {
		return fmt.Errorf("exp-09: flush after %d events: %w", sent, err)
	}

	lines := []string{
		fmt.Sprintf("producer\t%s", describe(cfg)),
		fmt.Sprintf("events produced\t%d", sent),
		fmt.Sprintf("acknowledged\t%d", acked.Load()),
		fmt.Sprintf("failed\t%d", failed.Load()),
		fmt.Sprintf("PRODUCED\t%d", sent),
	}
	if reason, ok := lastFailure.Load().(string); ok {
		lines = append(lines, fmt.Sprintf("last failure\t%s", reason))
	}
	return render(out, lines)
}

func stream(ctx context.Context, client *kgo.Client, cfg *settings, promise func(*kgo.Record, error)) (int, error) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	stop := time.After(cfg.duration)

	seq := 0
	for {
		select {
		case <-ctx.Done():
			return seq, fmt.Errorf("exp-09: stopped producing after %d events: %w", seq, ctx.Err())
		case <-stop:
			return seq, nil
		case <-ticker.C:
			for range cfg.perTick {
				next, err := startPayment(ctx, client, seq, promise)
				if err != nil {
					return seq, err
				}
				seq = next
			}
		}
	}
}

// startPayment hands the client a capture and then its refund, keyed by the payment so
// both belong to the same ordered stream. It returns the next seq.
func startPayment(ctx context.Context, client *kgo.Client, seq int, promise func(*kgo.Record, error)) (int, error) {
	payment := fmt.Sprintf("pay-%07d", seq/2)
	for _, status := range []string{statusCaptured, statusRefunded} {
		value, err := json.Marshal(event{PaymentID: payment, Status: status, Seq: seq})
		if err != nil {
			return seq, fmt.Errorf("exp-09: encode event %d: %w", seq, err)
		}
		client.Produce(ctx, &kgo.Record{Topic: topic, Key: []byte(payment), Value: value}, promise)
		seq++
	}
	return seq, nil
}

func verify(ctx context.Context, cfg *settings, out io.Writer) error {
	records, err := labkit.ReadAll(ctx, strings.Split(cfg.brokers, ","), topic, readStall)
	if err != nil {
		return err
	}

	delivered := make([]event, 0, len(records))
	for _, record := range records {
		var e event
		if err := json.Unmarshal(record.Value, &e); err != nil {
			return fmt.Errorf("%w at offset %d: %w", errDecode, record.Offset, err)
		}
		delivered = append(delivered, e)
	}

	got := analyse(delivered, cfg.produced)
	return render(out, report(cfg, got))
}

// report states the one expectation this experiment can hold a run to. The idempotent
// producer must keep order, duplicates and completeness exactly, whatever the cluster did.
// The plain producer has no such promise to break, so its numbers are reported as found.
func report(cfg *settings, got order) []string {
	verdict := "reported as found — the plain producer promises nothing to check it against"
	if cfg.producer == producerIdempotent {
		verdict = "held, as it must"
		if !got.held() {
			verdict = "BROKEN — the idempotent producer is supposed to make this impossible"
		}
	}
	return []string{
		fmt.Sprintf("producer\t%s", describe(cfg)),
		fmt.Sprintf("produced\t%d, read back %d, distinct %d", got.Produced, got.Read, got.Distinct),
		fmt.Sprintf("  out of order\t%d", got.Inversions),
		fmt.Sprintf("  refund before capture\t%d payments", got.RefundedFirst),
		fmt.Sprintf("  duplicated\t%d", got.Duplicates),
		fmt.Sprintf("  missing\t%d", got.Missing),
		fmt.Sprintf("  verdict\t%s", verdict),
	}
}

func describe(cfg *settings) string {
	if cfg.producer == producerPlain {
		return "idempotence off, 5 requests in flight, acks=all"
	}
	return "idempotent, acks=all"
}

// leader prints the leader of partition 0 and one follower, so the script can freeze the
// follower and leave the leader alone.
func leader(ctx context.Context, cfg *settings, out io.Writer) error {
	admin, closeAdmin, err := adminFor(cfg)
	if err != nil {
		return err
	}
	defer closeAdmin()

	part, err := labkit.Describe(ctx, admin, topic, 0)
	if err != nil {
		return err
	}
	follower := int32(-1)
	for _, replica := range part.Replicas {
		if replica != part.Leader {
			follower = replica
			break
		}
	}
	return render(out, []string{
		fmt.Sprintf("LEADER_P0\t%d", part.Leader),
		fmt.Sprintf("FOLLOWER_P0\t%d", follower),
		fmt.Sprintf("in-sync replicas\t%v of %v", part.ISR, part.Replicas),
	})
}

func await(ctx context.Context, cfg *settings, out io.Writer, what string, reached func(isr, replicas int) bool) error {
	admin, closeAdmin, err := adminFor(cfg)
	if err != nil {
		return err
	}
	defer closeAdmin()

	waited, part, err := labkit.AwaitISR(ctx, admin, topic, 0, reached)
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
		return nil, nil, fmt.Errorf("exp-09: kafka client: %w", err)
	}
	return kadm.NewClient(client), client.Close, nil
}

func render(out io.Writer, lines []string) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("exp-09: write report: %w", err)
		}
	}
	return table.Flush()
}
