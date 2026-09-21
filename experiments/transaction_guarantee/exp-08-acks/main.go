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
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"

	maxRecords     = 1_000_000
	maxRecordBytes = 1 << 20
	readStall      = 10 * time.Second
)

var phases = []string{"write-acks-one", "count", "write-acks-all", "leader", "await-shrunk", "await-full"}

var (
	errPhase = errors.New("exp-08: -phase must be write-acks-one, count, write-acks-all, leader, await-shrunk or await-full")
	errShape = errors.New("exp-08: -records out of range")
	errTopic = errors.New("exp-08: -topic is required")
)

type settings struct {
	brokers     string
	topic       string
	phase       string
	records     int
	recordBytes int
	timeout     time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-08 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.topic, "topic", "", "topic this phase works on")
	flag.StringVar(&cfg.phase, "phase", "", strings.Join(phases, ", "))
	flag.IntVar(&cfg.records, "records", 500, "records this phase writes")
	flag.IntVar(&cfg.recordBytes, "record-bytes", 4096, "payload size, which decides whether one fetch can carry the whole log")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "deadline for this phase")
	flag.Parse()

	switch {
	case !slices.Contains(phases, cfg.phase):
		return errPhase
	case cfg.topic == "":
		return errTopic
	case cfg.records <= 0 || cfg.records > maxRecords:
		return fmt.Errorf("%w: -records is %d", errShape, cfg.records)
	case cfg.recordBytes <= 0 || cfg.recordBytes > maxRecordBytes:
		return fmt.Errorf("%w: -record-bytes is %d", errShape, cfg.recordBytes)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	return measure(ctx, &cfg, os.Stdout)
}

func measure(ctx context.Context, cfg *settings, out io.Writer) error {
	switch cfg.phase {
	case "write-acks-one":
		return writeAcksOne(ctx, cfg, out)
	case "write-acks-all":
		return writeAcksAll(ctx, cfg, out)
	case "count":
		return count(ctx, cfg, out)
	case "await-shrunk":
		return awaitShrunk(ctx, cfg, out)
	case "await-full":
		return awaitFull(ctx, cfg, out)
	default:
		return leader(ctx, cfg, out)
	}
}

// writeAcksOne asks the leader alone to acknowledge. Every record this phase reports as
// accepted has been promised to the caller, which is what makes losing it afterwards a
// different thing from failing to write it.
func writeAcksOne(ctx context.Context, cfg *settings, out io.Writer) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.RequiredAcks(kgo.LeaderAck()),
		// Idempotent production requires acks=all, so asking for one without the other is
		// a configuration error rather than a weaker guarantee.
		kgo.DisableIdempotentWrite(),
		// franz-go compresses with snappy by default, unlike the Java client. The padding
		// below is repetitive, so with compression on, 8 MB of records became a 600 KB log
		// and RECORD_BYTES stopped meaning what it says. The bytes reach the disk as written.
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		return fmt.Errorf("exp-08: kafka client: %w", err)
	}
	defer client.Close()

	accepted, failure := write(ctx, client, cfg)
	return render(out, []string{
		"acks\t1, the leader answers on its own append",
		fmt.Sprintf("acknowledged\t%d of %d%s", accepted, cfg.records, note(failure)),
	})
}

// writeAcksAll asks the whole in-sync set. On a topic with min.insync.replicas above the
// number of live replicas the cluster refuses, and a refusal is the result here, not an
// error: the caller is told no instead of being told yes and lied to.
func writeAcksAll(ctx context.Context, cfg *settings, out io.Writer) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Without this a refusal is retried until the deadline instead of being reported.
		kgo.RecordRetries(1),
	)
	if err != nil {
		return fmt.Errorf("exp-08: kafka client: %w", err)
	}
	defer client.Close()

	accepted, failure := write(ctx, client, cfg)
	return render(out, []string{
		"acks\tall, every replica in the in-sync set",
		fmt.Sprintf("accepted\t%d of %d%s", accepted, cfg.records, note(failure)),
		fmt.Sprintf("refused outright\t%s", refusal(failure)),
	})
}

// refusal names the one failure this half exists to show. Any other error — a timeout, a
// leader that is not available — is an instrument or cluster fault, and reporting it as a
// refusal would claim the setting worked when the run never reached it. The AfterAppend
// variant counts too: the leader appended, the in-sync set then shrank, and the producer
// was still told no — which is the same promise kept.
func refusal(err error) string {
	switch {
	case errors.Is(err, kerr.NotEnoughReplicas), errors.Is(err, kerr.NotEnoughReplicasAfterAppend):
		return "yes, NOT_ENOUGH_REPLICAS"
	case err != nil:
		return fmt.Sprintf("no — failed for another reason: %v", err)
	default:
		return "no"
	}
}

func write(ctx context.Context, client *kgo.Client, cfg *settings) (int, error) {
	// Every key is distinct, so count can tell a lost record from a duplicated one.
	padding := strings.Repeat("x", cfg.recordBytes)
	batch := make([]*kgo.Record, 0, cfg.records)
	for i := range cfg.records {
		batch = append(batch, &kgo.Record{
			Topic: cfg.topic,
			Key:   fmt.Appendf(nil, "pay-%05d", i),
			Value: fmt.Appendf(nil, `{"seq":%d,"pad":%q}`, i, padding),
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

func note(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf(" — %v", err)
}
