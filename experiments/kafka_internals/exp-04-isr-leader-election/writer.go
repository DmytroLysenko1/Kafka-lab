package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var errNoKill = errors.New("exp-04: the writer was never told when the broker was killed")

// ack is one record the writer sent and what became of it.
type ack struct {
	sent  time.Time
	acked time.Time
	err   error
}

// across is what a caller writing through the kill would have seen: the longest stretch with
// no acknowledgement at all, and the longest any one record waited for its own.
type across struct {
	sent, accepted, failed int
	gap                    time.Duration
	gapStart               time.Time
	slowest                time.Duration
}

// writer prices the pause the other phases step around: it writes to partition 0 — the one
// whose leader the run kills — at a steady rate from before the kill until after the
// re-election, so the kill lands with records in flight.
func writer(ctx context.Context, cfg *settings, out io.Writer) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return fmt.Errorf("exp-04: kafka client: %w", err)
	}
	defer client.Close()

	acks, err := writeSteadily(ctx, client, cfg.rate, cfg.duration)
	if err != nil {
		return err
	}
	killed, err := killedAt(cfg.killedFile)
	if err != nil {
		return err
	}
	return reportAcross(out, summarise(acks), killed)
}

// writeSteadily sends one record per tick for the whole duration and waits for every answer.
func writeSteadily(ctx context.Context, client *kgo.Client, rate int, duration time.Duration) ([]ack, error) {
	var (
		mu   sync.Mutex
		acks []ack
	)
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()
	stop := time.After(duration)

	for seq := 0; ; seq++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-stop:
			if err := client.Flush(ctx); err != nil {
				return nil, fmt.Errorf("exp-04: wait for the writer's last records: %w", err)
			}
			// Flush returns only after every promise has run, so nothing appends after this.
			mu.Lock()
			answered := acks
			mu.Unlock()
			return answered, nil
		case <-ticker.C:
			sent := time.Now()
			record := &kgo.Record{Partition: 0, Key: fmt.Appendf(nil, "across-%05d", seq), Value: fmt.Appendf(nil, `{"seq":%d}`, seq)}
			client.Produce(ctx, record, func(_ *kgo.Record, err error) {
				mu.Lock()
				defer mu.Unlock()
				acks = append(acks, ack{sent: sent, acked: time.Now(), err: err})
			})
		}
	}
}

func summarise(acks []ack) across {
	result := across{sent: len(acks)}
	var times []time.Time
	for _, answered := range acks {
		if answered.err != nil {
			result.failed++
			continue
		}
		result.accepted++
		result.slowest = max(result.slowest, answered.acked.Sub(answered.sent))
		times = append(times, answered.acked)
	}
	slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
	for i := 1; i < len(times); i++ {
		if gap := times[i].Sub(times[i-1]); gap > result.gap {
			result.gap, result.gapStart = gap, times[i-1]
		}
	}
	return result
}

// killedAt reads the kill's unix milliseconds, which the shell writes the moment it kills.
func killedAt(path string) (time.Time, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // the path is this experiment's own flag, written by run.sh
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %w", errNoKill, err)
	}
	millis, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %w", errNoKill, err)
	}
	return time.UnixMilli(millis), nil
}

func reportAcross(out io.Writer, result across, killed time.Time) error {
	return render(out, []string{
		"phase\twriting through the kill, to partition 0",
		fmt.Sprintf("writes accepted\t%d of %d with acks=all, %d failed", result.accepted, result.sent, result.failed),
		fmt.Sprintf("longest gap between acks\t%.1f s, %s", result.gap.Seconds(), relativeTo(result.gapStart, killed)),
		fmt.Sprintf("slowest single write\t%.1f s from send to ack", result.slowest.Seconds()),
	})
}

// relativeTo places the start of the gap against the kill. The shell stamps the kill after
// docker kill returns, so the last ack before it can land a little before the stamp.
func relativeTo(start, killed time.Time) string {
	offset := start.Sub(killed)
	if offset < 0 {
		return fmt.Sprintf("from the last ack, %.1f s before the kill was stamped", -offset.Seconds())
	}
	return fmt.Sprintf("from %.1f s after the kill", offset.Seconds())
}
