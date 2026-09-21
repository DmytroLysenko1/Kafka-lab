package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// exp-17b: the same producer with nothing holding it back. Each cell produces as fast as it
// can for the cell's duration, limited only by franz-go's buffer of 10 000 records, so
// throughput and the codecs' CPU cost finally differ.
var saturationLingers = []time.Duration{0, 10 * time.Millisecond}

// poolSize payloads are generated before any cell runs and cycled through, so generating
// JSON is not in the CPU measured and every codec compresses the same bytes in the same
// order.
const poolSize = 200_000

type record struct {
	key, value []byte
}

type saturated struct {
	acked    int
	failed   int
	elapsed  time.Duration
	cpu      time.Duration
	lat      latencies
	sentWire wire
}

func saturationSweep(ctx context.Context, cfg *settings, out io.Writer) error {
	pool := payloadPool(poolSize)
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', tabwriter.AlignRight)
	if _, err := fmt.Fprintln(table, "linger\tbatch\tcodec\tacked/s\tMB/s in\tMB/s wire\twire ratio\tCPU ms/MB\tp50\tp99\tfailed\t"); err != nil {
		return fmt.Errorf("exp-17b: write report: %w", err)
	}
	for _, linger := range saturationLingers {
		for _, c := range codecs {
			cell := point{linger: linger, batch: defaultBatchBytes, codec: c}
			got, err := saturate(ctx, cfg, cell, pool)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintln(table, saturationRow(cell, &got)); err != nil {
				return fmt.Errorf("exp-17b: write report: %w", err)
			}
		}
	}
	return table.Flush()
}

func payloadPool(n int) []record {
	source := newPayloads()
	pool := make([]record, n)
	for i := range pool {
		pool[i].key, pool[i].value = source.next()
	}
	return pool
}

// saturate runs one cell flat out. CPU is the whole process's user and system time across
// the cell, so it includes the client's own overhead; the "none" row is that overhead, and
// a codec's cost is its row minus that one.
func saturate(ctx context.Context, cfg *settings, cell point, pool []record) (saturated, error) {
	sent := &wireHook{}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ProducerLinger(cell.linger),
		kgo.ProducerBatchMaxBytes(cell.batch),
		kgo.ProducerBatchCompression(cell.codec.codec),
		kgo.WithHooks(sent),
	)
	if err != nil {
		return saturated{}, fmt.Errorf("exp-17b: kafka client for %s: %w", cell.codec.name, err)
	}

	got, err := flatOut(ctx, client, cfg.duration, pool)
	client.Close()
	if err != nil {
		return saturated{}, err
	}
	// The bytes are read only once the hook has seen every acknowledged record; Close does
	// not wait for the goroutine franz-go calls it from.
	if err := sent.await(ctx, got.acked, hookWait); err != nil {
		return saturated{}, err
	}
	got.sentWire = sent.total()
	return got, nil
}

func flatOut(ctx context.Context, client *kgo.Client, d time.Duration, pool []record) (saturated, error) {
	rec := &recorder{lat: make(latencies, 0, 1<<20)}
	cpuBefore, err := cpuTime()
	if err != nil {
		return saturated{}, err
	}
	started := time.Now()
	deadline := started.Add(d)
	for i := 0; time.Now().Before(deadline); i++ {
		r := pool[i%len(pool)]
		handed := time.Now()
		client.Produce(ctx, &kgo.Record{Topic: topic, Key: r.key, Value: r.value}, func(_ *kgo.Record, err error) {
			rec.record(time.Since(handed), err)
		})
		if ctx.Err() != nil {
			return saturated{}, fmt.Errorf("exp-17b: stopped producing: %w", ctx.Err())
		}
	}
	if err := client.Flush(ctx); err != nil {
		return saturated{}, fmt.Errorf("exp-17b: flush: %w", err)
	}
	elapsed := time.Since(started)
	cpuAfter, err := cpuTime()
	if err != nil {
		return saturated{}, err
	}

	counted := rec.snapshot()
	return saturated{acked: len(counted.lat), failed: counted.failed, elapsed: elapsed, cpu: cpuAfter - cpuBefore, lat: counted.lat}, nil
}

func cpuTime() (time.Duration, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, fmt.Errorf("exp-17b: getrusage: %w", err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano()), nil
}

func saturationRow(cell point, got *saturated) string {
	seconds := got.elapsed.Seconds()
	in := float64(got.sentWire.Uncompressed) / 1e6
	return fmt.Sprintf("%s\t%s\t%s\t%.0f\t%.1f\t%.1f\t%.2f\t%.1f\t%s\t%s\t%d\t",
		cell.linger, batchLabel(cell.batch), cell.codec.name,
		float64(got.acked)/seconds, in/seconds, float64(got.sentWire.Compressed)/1e6/seconds,
		got.sentWire.ratio(), float64(got.cpu.Milliseconds())/max(in, 1e-9),
		got.lat.percentile(0.50).Round(100*time.Microsecond), got.lat.percentile(0.99).Round(100*time.Microsecond),
		got.failed)
}
