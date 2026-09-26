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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	topic          = "exp14.payments"
	partitions     = 6
	tick           = 5 * time.Millisecond
	maxRate        = 50_000
	pollRecords    = 200
	maxWork        = 50 * time.Millisecond
	flushTimeout   = 30 * time.Second
	// perSecondPerTick is one record every tick: the rate must be a multiple of it, or the
	// integer division that spreads it over ticks silently produces less than asked.
	perSecondPerTick = int(time.Second / tick)
)

var (
	errStrategy = errors.New("exp-14: -strategy must be eager, cooperative or server")
	errShape    = errors.New("exp-14: -rate, -work, -join-after, -leave-after or -duration out of range")
	errUnacked  = errors.New("exp-14: records were not acknowledged, so a gap in a partition is not the consumer's")
)

type settings struct {
	brokers   string
	strategy  string
	rate      int
	work      time.Duration
	block     bool
	joinAfter time.Duration
	// leaveAfter closes the second consumer again, so the run measures a member leaving
	// rather than joining; 0 leaves it in for the whole run.
	leaveAfter time.Duration
	// rejoinAfter brings the second consumer back that long after it left, which is what a
	// rolling deploy looks like to the group; 0 leaves it gone.
	rejoinAfter time.Duration
	// static gives both members a group.instance.id. franz-go then sends no LeaveGroup when
	// a member closes, so its partitions stay assigned to it until the session times out.
	static         bool
	sessionTimeout time.Duration
	duration       time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-14 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.strategy, "strategy", "", "eager, cooperative or server")
	flag.IntVar(&cfg.rate, "rate", 1200, "records produced per second, a multiple of 200, spread evenly over the partitions")
	flag.DurationVar(&cfg.work, "work", 0, "time the handler spends on each record")
	flag.BoolVar(&cfg.block, "block", false, "BlockRebalanceOnPoll: a rebalance waits for the batch in hand to be handled")
	flag.DurationVar(&cfg.joinAfter, "join-after", 15*time.Second, "when the second consumer joins the group")
	flag.DurationVar(&cfg.leaveAfter, "leave-after", 0, "when the second consumer leaves again; 0 keeps it for the whole run")
	flag.DurationVar(&cfg.rejoinAfter, "rejoin-after", 0, "bring the second consumer back this long after it left; 0 leaves it gone")
	flag.BoolVar(&cfg.static, "static", false, "static membership: give both members a group.instance.id")
	flag.DurationVar(&cfg.sessionTimeout, "session-timeout", 12*time.Second, "how long a member may go unheard before the group drops it")
	flag.DurationVar(&cfg.duration, "duration", 35*time.Second, "how long the whole run lasts")
	flag.Parse()

	if _, err := strategyOptions(cfg.strategy); err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return experiment(ctx, &cfg, os.Stdout)
}

// validate keeps the measured moment — the join, or the leave when there is one — far
// enough from the run's edges for the baseline window before it and the window after it.
func (cfg *settings) validate() error {
	switch {
	case cfg.rate <= 0 || cfg.rate > maxRate || cfg.rate%perSecondPerTick != 0:
		return fmt.Errorf("%w: -rate %d", errShape, cfg.rate)
	case cfg.work < 0 || cfg.work > maxWork:
		return fmt.Errorf("%w: -work %s", errShape, cfg.work)
	}
	return cfg.validateSchedule()
}

// validateSchedule places the join, the leave and the run's end so that every window the
// report needs falls inside the run: ten seconds of baseline before the measured moment,
// and fifteen seconds after it.
func (cfg *settings) validateSchedule() error {
	switch {
	case cfg.joinAfter <= 0:
		return fmt.Errorf("%w: -join-after %s", errShape, cfg.joinAfter)
	case cfg.leaveAfter != 0 && cfg.leaveAfter < cfg.joinAfter+baselineWindow+joinMargin:
		return fmt.Errorf("%w: -leave-after %s", errShape, cfg.leaveAfter)
	case cfg.rejoinAfter != 0 && cfg.leaveAfter == 0:
		return fmt.Errorf("%w: -rejoin-after needs -leave-after", errShape)
	case cfg.measuredAt() < baselineWindow+joinMargin:
		return fmt.Errorf("%w: -join-after %s", errShape, cfg.joinAfter)
	case cfg.duration < cfg.measuredAt()+afterJoinWindow:
		return fmt.Errorf("%w: -duration %s", errShape, cfg.duration)
	}
	return nil
}

// measuredAt is the moment the report's windows are placed around.
func (cfg *settings) measuredAt() time.Duration {
	if cfg.leaveAfter > 0 {
		return cfg.leaveAfter
	}
	return cfg.joinAfter
}

// strategyOptions is the whole difference between the three runs. Eager has to be asked for:
// franz-go defaults to cooperative-sticky, so a run left at its defaults would measure
// cooperative twice. The server run is KIP-848, where the broker assigns partitions and
// the client's balancer only names the assignor.
func strategyOptions(name string) ([]kgo.Opt, error) {
	switch name {
	case "eager":
		return []kgo.Opt{kgo.Balancers(kgo.RangeBalancer())}, nil
	case "cooperative":
		return []kgo.Opt{kgo.Balancers(kgo.CooperativeStickyBalancer())}, nil
	case "server":
		return []kgo.Opt{kgo.Balancers(kgo.RangeBalancer()), kgo.ServerSideBalancer()}, nil
	default:
		return nil, errStrategy
	}
}

func describe(name string) string {
	switch name {
	case "eager":
		return "eager: RangeBalancer, classic protocol, every partition revoked on a rebalance"
	case "cooperative":
		return "cooperative: CooperativeStickyBalancer, classic protocol, only moving partitions revoked"
	default:
		return "server: KIP-848, the broker assigns (range assignor), incremental"
	}
}

func describeHandler(cfg *settings) string {
	if cfg.block {
		return fmt.Sprintf("%s a record, rebalances blocked while a batch of up to %d is handled", cfg.work, pollRecords)
	}
	return fmt.Sprintf("%s a record, franz-go's default: rebalances run beside the handler", cfg.work)
}

// experiment produces a steady stream, starts one consumer, adds a second after joinAfter,
// and reports how long each partition went unprocessed around the join.
func experiment(ctx context.Context, cfg *settings, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	group := fmt.Sprintf("exp14-%s-%d", cfg.strategy, time.Now().UnixNano())
	line := newTimeline(time.Now())

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return produce(gctx, cfg)
	})
	g.Go(func() error {
		return consume(gctx, cfg, group, "A", line)
	})
	g.Go(func() error {
		return second(gctx, cfg, group, line)
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return render(out, report(cfg, group, line.snapshot()))
}

// second is the member whose arrival — or departure — the run measures. It joins after
// joinAfter, and with leaveAfter set it closes again at that mark, which is the moment the
// report's windows are placed around.
func second(ctx context.Context, cfg *settings, group string, line *timeline) error {
	if !sleep(ctx, cfg.joinAfter) {
		return nil
	}
	if cfg.leaveAfter == 0 {
		line.markMeasured()
		return consume(ctx, cfg, group, "B", line)
	}

	member, leave := context.WithCancel(ctx)
	defer leave()
	go func() {
		if sleep(ctx, cfg.leaveAfter-cfg.joinAfter) {
			line.markMeasured()
			leave()
		}
	}()
	if err := consume(member, cfg, group, "B", line); err != nil {
		return err
	}
	if cfg.rejoinAfter == 0 {
		// The member is gone; the run keeps going so its partitions can be seen coming back.
		<-ctx.Done()
		return nil
	}
	if !sleep(ctx, cfg.rejoinAfter) {
		return nil
	}
	// Back with the same identity if the run is static, a stranger if it is not.
	return consume(ctx, cfg, group, "B", line)
}

// produce spreads records evenly over the partitions by number, so every partition has the
// same steady flow and a gap in one of them is the consumer's, not the producer's.
func produce(ctx context.Context, cfg *settings) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return fmt.Errorf("exp-14: producer client: %w", err)
	}
	defer client.Close()

	// The whole result of this experiment is "the longest stretch nothing of partition p was
	// handled". A record the producer failed to write leaves exactly that stretch, so without
	// counting the failures a producer problem is indistinguishable from the consumer stall
	// being measured — and the comment above would be false.
	var failed atomic.Int64
	acked := func(_ *kgo.Record, err error) {
		if err != nil {
			failed.Add(1)
		}
	}

	// The records get a context the end of the run cannot cancel. franz-go fails a buffered
	// record whose context is done, so with the run's own context the last tick's records
	// were failed at shutdown and counted as a gap in the stream — 15 and 18 of them in the
	// two runs that surfaced it — when nothing had gone wrong. The run ending stops new
	// records; the ones already sent are flushed and must still be acknowledged.
	sending := context.WithoutCancel(ctx)

	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	perTick := cfg.rate / perSecondPerTick
	for seq := int64(0); ; {
		select {
		case <-ctx.Done():
			return flushed(ctx, client, &failed)
		case <-ticker.C:
			for range perTick {
				client.Produce(sending, &kgo.Record{
					Partition: int32(seq % partitions),
					Value:     strconv.AppendInt(nil, seq, 10),
				}, acked)
				seq++
			}
		}
	}
}

// flushed waits for the records still in flight and refuses a stream with a gap in it. The
// run's context is already cancelled by the time this runs — that cancellation is what
// brought us here — so the flush detaches from it while keeping its values, and bounds
// itself instead of inheriting a deadline that has already passed.
func flushed(ctx context.Context, client *kgo.Client, failed *atomic.Int64) error {
	draining, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
	defer cancel()

	if err := client.Flush(draining); err != nil {
		return fmt.Errorf("exp-14: flush: %w", err)
	}
	if n := failed.Load(); n > 0 {
		return fmt.Errorf("%w: %d of them", errUnacked, n)
	}
	return nil
}

// consume is one member of the group. It records when each record was handled and every
// assignment change the group put it through; the run's deadline is the only way it stops.
//
// With -block the member holds rebalances off while it handles a batch, which is how the
// Java consumer behaves — it rebalances only inside poll — and what makes a rebalance wait
// for work in hand. Without it franz-go revokes in the background, and a handler still busy
// with a revoked partition's records goes on handling them.
func consume(ctx context.Context, cfg *settings, group, member string, line *timeline) error {
	strategy, err := strategyOptions(cfg.strategy)
	if err != nil {
		return err
	}
	opts := slices.Concat(strategy, []kgo.Opt{
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		// Only handled records are ever committed. Without this the revoke callback below
		// would have to commit everything the last poll returned, handled or not — which is
		// exactly the loss window franz-go's own revoke callback avoids by committing the
		// previous poll instead.
		kgo.AutoCommitMarks(),
		kgo.OnPartitionsAssigned(line.change(member, "assigned")),
		kgo.OnPartitionsRevoked(line.revoked(member)),
		kgo.OnPartitionsLost(line.change(member, "lost")),
		kgo.SessionTimeout(cfg.sessionTimeout),
	})
	if cfg.static {
		opts = append(opts, kgo.InstanceID(group+"-"+member))
	}
	if cfg.block {
		opts = append(opts, kgo.BlockRebalanceOnPoll())
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("exp-14: consumer %s: %w", member, err)
	}
	defer client.Close()
	// Runs before Close: a blocked rebalance would otherwise hold Close's leave forever.
	defer client.AllowRebalance()

	for {
		fetches := client.PollRecords(ctx, pollRecords)
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("exp-14: consumer %s: %w", member, err)
		}
		handle(ctx, fetches, cfg.work, member, line)
		client.MarkCommitRecords(fetches.Records()...)
		client.AllowRebalance()
	}
}

// handle spends work on each record and records when it was done. It stops at a cancelled
// context rather than finishing the batch: a batch of 200 records is seconds of work, and
// the run's deadline should not have to wait for it.
func handle(ctx context.Context, fetches kgo.Fetches, work time.Duration, member string, line *timeline) {
	stopped := false
	fetches.EachRecord(func(record *kgo.Record) {
		if stopped {
			return
		}
		seq, err := strconv.ParseInt(string(record.Value), 10, 64)
		if err != nil {
			return
		}
		if !sleep(ctx, work) {
			stopped = true
			return
		}
		line.handled(member, record.Partition, seq, time.Now())
	})
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d == 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
