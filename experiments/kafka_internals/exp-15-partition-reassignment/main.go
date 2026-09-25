// exp-15 raises a topic's replication factor while producers are writing to it, once at
// full speed and once with a throttle, and reports what each choice costs: how long the
// move takes, and what the producers waiting behind it saw.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var errPhase = errors.New("exp-15: phase must be preload or measure")

const (
	recordBytes  = 1024
	produceEvery = 10 * time.Millisecond
)

// A fixed seed so two runs move the same bytes; the point of the randomness is that the
// broker cannot compress it away, not that it is unpredictable.
var random = rand.New(rand.NewPCG(15, 2026)) //nolint:gosec // payload bytes, not a secret

type settings struct {
	phase     string
	topic     string
	megabytes int
	window    time.Duration
	moveAfter time.Duration
	throttle  int
}

// latency is one acknowledged write, timed from the call to the ack.
type latency struct {
	at   time.Duration
	took time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("exp-15 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var s settings
	flag.StringVar(&s.phase, "phase", "", "preload or measure")
	flag.StringVar(&s.topic, "topic", "exp15.reassign", "the topic to move")
	flag.IntVar(&s.megabytes, "megabytes", 60, "how much to preload, so the move has something to copy")
	flag.DurationVar(&s.window, "window", 120*time.Second, "how long to keep producing")
	flag.DurationVar(&s.moveAfter, "move-after", 15*time.Second, "when to start the reassignment")
	flag.IntVar(&s.throttle, "throttle", 0, "bytes per second for the move; 0 means no throttle")
	flag.Parse()

	ctx := context.Background()
	switch s.phase {
	case "preload":
		return preload(ctx, &s, os.Stdout)
	case "measure":
		return measure(ctx, &s, os.Stdout)
	default:
		return fmt.Errorf("phase %q: %w", s.phase, errPhase)
	}
}

func brokers() []string {
	return strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
}

func producer(topic string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
}

// preload fills the topic so the reassignment has something to copy. A move of an empty
// topic finishes before anyone can measure it, which is how reassignment gets its
// reputation for being free.
func preload(ctx context.Context, s *settings, out io.Writer) error {
	client, err := producer(s.topic)
	if err != nil {
		return err
	}
	defer client.Close()

	records := s.megabytes * 1024 * 1024 / recordBytes
	batch := make([]*kgo.Record, 0, 1000)
	for i := range records {
		batch = append(batch, &kgo.Record{Value: noise()})
		if len(batch) < cap(batch) && i != records-1 {
			continue
		}
		if err := client.ProduceSync(ctx, batch...).FirstErr(); err != nil {
			return fmt.Errorf("preload: %w", err)
		}
		batch = batch[:0]
	}

	stored, err := onDisk(ctx, s.topic)
	if err != nil {
		return err
	}

	written := &lines{out: out}
	written.printf("preloaded %d MiB as %d records; %s on disk across the cluster\n",
		s.megabytes, records, mib(stored))
	return written.err
}

// noise is incompressible on purpose. The first version of this run filled its records
// with a repeating pattern, and 60 MiB of that landed as 2 MiB per broker — the move it
// was supposed to measure had almost nothing to copy and finished in two seconds.
func noise() []byte {
	payload := make([]byte, recordBytes)
	for i := range payload {
		payload[i] = byte(random.Uint32()) //nolint:gosec // payload bytes, not a secret
	}
	return payload
}

func mib(bytes int64) string {
	return fmt.Sprintf("%.1f MiB", float64(bytes)/1024/1024)
}

func measure(ctx context.Context, s *settings, out io.Writer) error {
	client, err := producer(s.topic)
	if err != nil {
		return err
	}
	defer client.Close()

	running, stop := context.WithTimeout(ctx, s.window)
	defer stop()

	started := time.Now()
	writes := make(chan latency, 1<<16)

	go func() {
		defer close(writes)
		produce(running, client, started, writes)
	}()

	before, err := onDisk(ctx, s.topic)
	if err != nil {
		return err
	}

	move, err := reassign(running, s, started)
	if err != nil {
		return err
	}

	after, err := onDisk(ctx, s.topic)
	if err != nil {
		return err
	}

	timed := make([]latency, 0, 1<<16)
	for write := range writes {
		timed = append(timed, write)
	}

	return report(out, s, timed, move, after-before)
}

// produce writes one record at a time and times the acknowledgement. One at a time is the
// point: a batched producer would hide the stall behind its own buffering, and the
// question is what a caller waiting for an ack sees while replicas are being copied.
func produce(ctx context.Context, client *kgo.Client, started time.Time, writes chan<- latency) {
	ticker := time.NewTicker(produceEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			at := time.Since(started)
			sent := time.Now()
			if err := client.ProduceSync(ctx, &kgo.Record{Value: noise()}).FirstErr(); err != nil {
				continue
			}
			select {
			case writes <- latency{at: at, took: time.Since(sent)}:
			default:
			}
		}
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := min(int(float64(len(sorted))*p), len(sorted)-1)
	return sorted[index]
}

func quantiles(timed []latency, from, until time.Duration) (count int, median, p95, worst time.Duration) {
	took := make([]time.Duration, 0, len(timed))
	for _, write := range timed {
		if write.at < from || write.at >= until {
			continue
		}
		took = append(took, write.took)
	}
	if len(took) == 0 {
		return 0, 0, 0, 0
	}
	slices.Sort(took)
	return len(took), percentile(took, 0.5), percentile(took, 0.95), took[len(took)-1]
}
