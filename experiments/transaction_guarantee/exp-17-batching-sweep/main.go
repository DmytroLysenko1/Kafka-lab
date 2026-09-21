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
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	topic          = "exp17.sweep"
	tick           = 5 * time.Millisecond
	maxRate        = 200_000
	// franz-go's own default batch ceiling, so one column of the sweep is "untouched".
	defaultBatchBytes = 1_000_012
)

var errShape = errors.New("exp-17: -rate or -duration out of range")

type codec struct {
	name  string
	codec kgo.CompressionCodec
}

// The grid. 5 ms is the Java client's default since Kafka 4.0 (KIP-1030; it was 0 before,
// which is the figure most tuning advice still quotes) and 10 ms is franz-go's, so both
// clients' current defaults are rows — read off the 4.3.1 client itself, see
// results/java-client-defaults.log. 0 shows what no lingering costs, and 50 ms is what bulk
// producers are told to try. The codecs are every one a payment team would consider, gzip
// aside, which nobody picks for a hot path.
var (
	lingers = []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond}
	batches = []int32{16 << 10, defaultBatchBytes}
	codecs  = []codec{
		{"none", kgo.NoCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	}
)

type settings struct {
	brokers  string
	rate     int
	duration time.Duration
	timeout  time.Duration
}

// point is one cell of the sweep.
type point struct {
	linger time.Duration
	batch  int32
	codec  codec
}

// outcome is what one cell measured.
type outcome struct {
	offered  int
	acked    int
	failed   int
	elapsed  time.Duration
	lat      latencies
	sentWire wire
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-17 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.IntVar(&cfg.rate, "rate", 20_000, "records offered per second, held constant across the sweep")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "how long each cell is offered load")
	flag.DurationVar(&cfg.timeout, "timeout", 20*time.Minute, "deadline for the whole sweep")
	flag.Parse()

	if cfg.rate <= 0 || cfg.rate > maxRate || cfg.duration <= 0 || cfg.duration > time.Minute {
		return fmt.Errorf("%w: -rate %d, -duration %s", errShape, cfg.rate, cfg.duration)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	return sweep(ctx, &cfg, os.Stdout)
}

func sweep(ctx context.Context, cfg *settings, out io.Writer) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', tabwriter.AlignRight)
	if _, err := fmt.Fprintln(table, "linger\tbatch\tcodec\toffered/s\tacked/s\tp50\tp99\trec/batch\tbytes/rec\twire ratio\t"); err != nil {
		return fmt.Errorf("exp-17: write report: %w", err)
	}

	for _, cell := range grid() {
		got, err := measure(ctx, cfg, cell)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(table, row(cell, &got)); err != nil {
			return fmt.Errorf("exp-17: write report: %w", err)
		}
	}
	return table.Flush()
}

// grid is every cell of the sweep, in the order the report prints them.
func grid() []point {
	cells := make([]point, 0, len(lingers)*len(batches)*len(codecs))
	for _, linger := range lingers {
		for _, batch := range batches {
			for _, c := range codecs {
				cells = append(cells, point{linger: linger, batch: batch, codec: c})
			}
		}
	}
	return cells
}

func row(cell point, got *outcome) string {
	seconds := got.elapsed.Seconds()
	return fmt.Sprintf("%s\t%s\t%s\t%.0f\t%.0f\t%s\t%s\t%.1f\t%.0f\t%.2f\t",
		cell.linger, batchLabel(cell.batch), cell.codec.name,
		float64(got.offered)/seconds, float64(got.acked)/seconds,
		got.lat.percentile(0.50).Round(100*time.Microsecond), got.lat.percentile(0.99).Round(100*time.Microsecond),
		got.sentWire.recordsPerBatch(), float64(got.sentWire.Compressed)/float64(max(got.sentWire.Records, 1)),
		got.sentWire.ratio())
}

func batchLabel(b int32) string {
	if b == defaultBatchBytes {
		return "default"
	}
	return fmt.Sprintf("%dKiB", b>>10)
}

// measure offers a fixed load to one configuration and records what came back. The load
// is the same for every cell, so a cell that cannot keep up shows it as acked below
// offered rather than as a faster run on less data.
func measure(ctx context.Context, cfg *settings, cell point) (outcome, error) {
	sent := &wireHook{}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ProducerLinger(cell.linger),
		kgo.ProducerBatchMaxBytes(cell.batch),
		kgo.ProducerBatchCompression(cell.codec.codec),
		kgo.WithHooks(sent),
	)
	if err != nil {
		return outcome{}, fmt.Errorf("exp-17: kafka client for %s: %w", cell.codec.name, err)
	}
	defer client.Close()

	rec := &recorder{lat: make(latencies, 0, cfg.rate*int(cfg.duration/time.Second))}
	started := time.Now()
	offered, err := offer(ctx, client, cfg, rec)
	if err != nil {
		return outcome{}, err
	}
	if err := client.Flush(ctx); err != nil {
		return outcome{}, fmt.Errorf("exp-17: flush %s: %w", cell.codec.name, err)
	}

	got := rec.snapshot()
	got.offered, got.elapsed, got.sentWire = offered, time.Since(started), sent.total()
	return got, nil
}

func offer(ctx context.Context, client *kgo.Client, cfg *settings, rec *recorder) (int, error) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	stop := time.After(cfg.duration)
	perTick := max(cfg.rate*int(tick)/int(time.Second), 1)
	source := newPayloads(17)

	offered := 0
	for {
		select {
		case <-ctx.Done():
			return offered, fmt.Errorf("exp-17: stopped after %d records: %w", offered, ctx.Err())
		case <-stop:
			return offered, nil
		case <-ticker.C:
			for range perTick {
				key, value := source.next()
				handed := time.Now()
				client.Produce(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}, func(_ *kgo.Record, err error) {
					rec.record(time.Since(handed), err)
				})
				offered++
			}
		}
	}
}

// recorder collects what the producer's callbacks report. Callbacks for different
// partitions run concurrently, so every access goes through the lock.
type recorder struct {
	mu     sync.Mutex
	lat    latencies
	acked  int
	failed int
}

func (r *recorder) record(took time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.failed++
		return
	}
	r.acked++
	r.lat = append(r.lat, took)
}

func (r *recorder) snapshot() outcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	return outcome{acked: r.acked, failed: r.failed, lat: r.lat}
}

// wireHook sums the client's own per-batch metrics: what actually went over the wire,
// compressed, rather than an estimate from the payload.
type wireHook struct {
	mu   sync.Mutex
	seen wire
}

func (h *wireHook) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, m kgo.ProduceBatchMetrics) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen.Batches++
	h.seen.Records += m.NumRecords
	h.seen.Uncompressed += m.UncompressedBytes
	h.seen.Compressed += m.CompressedBytes
}

func (h *wireHook) total() wire {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen
}
