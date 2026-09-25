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
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	topic          = "exp16.payments"
	partitions     = 3
	tick           = 5 * time.Millisecond
	pollRecords    = 100
	maxRate        = 5_000
	// rebalanceTimeout is short so that a member holding a rebalance is removed within the
	// outage rather than after it; the default is 60 s.
	rebalanceTimeout = 8 * time.Second
	sampleEvery      = time.Second
	// perSecondPerTick is one record every tick: the rate must be a multiple of it, or the
	// integer division that spreads it over ticks silently produces less than asked.
	perSecondPerTick = int(time.Second / tick)
)

var (
	errHandler = errors.New("exp-16: -handler must be block or pause")
	errShape   = errors.New("exp-16: the timings do not fit inside the run")
	errUnacked = errors.New("exp-16: records were not acknowledged, so the stream is not a baseline")
)

type settings struct {
	brokers    string
	handler    string
	rate       int
	outageAt   time.Duration
	outageFor  time.Duration
	joinAt     time.Duration
	produceFor time.Duration
	duration   time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-16 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.handler, "handler", "", "block or pause")
	flag.IntVar(&cfg.rate, "rate", 400, "records produced per second, a multiple of 200 so every 5 ms tick carries a whole number")
	flag.DurationVar(&cfg.outageAt, "outage-at", 15*time.Second, "when the downstream dependency goes down")
	flag.DurationVar(&cfg.outageFor, "outage-for", 20*time.Second, "how long it stays down")
	flag.DurationVar(&cfg.joinAt, "join-at", 20*time.Second, "when a second consumer joins the group")
	flag.DurationVar(&cfg.produceFor, "produce-for", 50*time.Second, "how long the stream runs")
	flag.DurationVar(&cfg.duration, "duration", 75*time.Second, "how long the whole run lasts, drain included")
	flag.Parse()

	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return experiment(ctx, &cfg, os.Stdout)
}

func (cfg *settings) validate() error {
	switch {
	case cfg.handler != handlerBlock && cfg.handler != handlerPause:
		return errHandler
	case cfg.rate <= 0 || cfg.rate > maxRate || cfg.rate%perSecondPerTick != 0:
		return fmt.Errorf("%w: -rate %d", errShape, cfg.rate)
	}
	return cfg.validateTimings()
}

// validateTimings keeps the join inside the outage and the stream running past it, or the
// run cannot show what happens to a group rebalancing while its dependency is down.
func (cfg *settings) validateTimings() error {
	outageEnd := cfg.outageAt + cfg.outageFor
	switch {
	case cfg.outageAt <= 0 || cfg.joinAt < cfg.outageAt || cfg.joinAt > outageEnd:
		return fmt.Errorf("%w: the join at %s must fall inside the outage", errShape, cfg.joinAt)
	case cfg.produceFor < outageEnd || cfg.duration <= cfg.produceFor:
		return fmt.Errorf("%w: -produce-for %s, -duration %s", errShape, cfg.produceFor, cfg.duration)
	}
	return nil
}

// experiment runs a stream, takes the dependency down, adds a member in the middle of the
// outage, and samples the group's lag once a second until the run ends.
func experiment(ctx context.Context, cfg *settings, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	start := time.Now()
	group := fmt.Sprintf("exp16-%s-%d", cfg.handler, start.UnixNano())
	line := newTimeline(start)
	dep := dependency{start: start, from: cfg.outageAt, until: cfg.outageAt + cfg.outageFor}
	var produced atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return produce(gctx, cfg, &produced) })
	g.Go(func() error { return consume(gctx, cfg, group, "A", dep, line) })
	g.Go(func() error {
		if !wait(gctx, cfg.joinAt) {
			return nil
		}
		return consume(gctx, cfg, group, "B", dep, line)
	})
	g.Go(func() error { return sampleLag(gctx, cfg, group, line) })
	if err := g.Wait(); err != nil {
		return err
	}
	recorded := line.snapshot()
	return render(out, report(cfg, group, produced.Load(), &recorded))
}

// produce writes a numbered stream spread evenly over the partitions and counts what the
// cluster acknowledged, which is what every record handled is checked against.
func produce(ctx context.Context, cfg *settings, produced *atomic.Int64) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return fmt.Errorf("exp-16: producer client: %w", err)
	}
	defer client.Close()

	var failed atomic.Int64
	acked := func(_ *kgo.Record, err error) {
		if err != nil {
			failed.Add(1)
			return
		}
		produced.Add(1)
	}

	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	stop := time.NewTimer(cfg.produceFor)
	defer stop.Stop()
	perTick := int64(cfg.rate / perSecondPerTick)
	for seq := int64(0); ; seq += perTick {
		select {
		case <-ctx.Done():
			return fmt.Errorf("exp-16: the run ended while producing: %w", ctx.Err())
		case <-stop.C:
			return finish(ctx, client, &failed)
		case <-ticker.C:
			for n := range perTick {
				client.Produce(ctx, &kgo.Record{Partition: int32((seq + n) % partitions), Value: strconv.AppendInt(nil, seq+n, 10)}, acked)
			}
		}
	}
}

// finish waits for every record to be acknowledged, and refuses a stream with a gap in it.
func finish(ctx context.Context, client *kgo.Client, failed *atomic.Int64) error {
	if err := client.Flush(ctx); err != nil {
		return fmt.Errorf("exp-16: flush: %w", err)
	}
	if n := failed.Load(); n > 0 {
		return fmt.Errorf("%w: %d of them", errUnacked, n)
	}
	return nil
}

// sampleLag reads the group's lag the way an operator would: the log end offsets against
// the group's committed offsets, once a second.
func sampleLag(ctx context.Context, cfg *settings, group string, line *timeline) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...))
	if err != nil {
		return fmt.Errorf("exp-16: admin client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	ticker := time.NewTicker(sampleEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			sampleOnce(ctx, admin, group, line)
		}
	}
}

// sampleOnce records one lag reading. A failed reading is counted rather than fatal: the
// run is about the consumers, and one missed sample does not change what they did.
func sampleOnce(ctx context.Context, admin *kadm.Client, group string, line *timeline) {
	lag, err := groupLag(ctx, admin, group)
	switch {
	case err == nil:
		line.sample(lag)
	case ctx.Err() == nil:
		line.sampleFailed()
	}
}

func groupLag(ctx context.Context, admin *kadm.Client, group string) (int64, error) {
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("exp-16: end offsets: %w", err)
	}
	committed, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		return 0, fmt.Errorf("exp-16: committed offsets: %w", err)
	}
	// A partition whose listing failed carries offset -1, and max(-1-at, 0) is 0. Left
	// unchecked, a reading that did not work is indistinguishable from no lag — in the one
	// experiment whose whole subject is lag. sampleOnce already counts a failed reading
	// apart from a zero one; it can only do that if this says so.
	if err := ends.Error(); err != nil {
		return 0, fmt.Errorf("exp-16: end offsets of %s: %w", topic, err)
	}
	if err := committed.Error(); err != nil {
		return 0, fmt.Errorf("exp-16: committed offsets of %s: %w", group, err)
	}

	var lag int64
	ends.Each(func(end kadm.ListedOffset) {
		at := int64(0)
		if c, ok := committed.Lookup(topic, end.Partition); ok {
			at = c.At
		}
		lag += max(end.Offset-at, 0)
	})
	return lag, nil
}

func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
