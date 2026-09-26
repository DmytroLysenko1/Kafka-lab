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
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"

	pollBatch    = 500
	fillerBytes  = 60
	maxEvents    = 10_000_000
	maxConsumers = 64
	leaveTimeout = 5 * time.Second
)

var (
	errShape       = errors.New("exp-02: event count out of range")
	errSkew        = errors.New("exp-02: -hot-share must be a percentage between 0 and 100")
	errGroupSizes  = errors.New("exp-02: -consumers must be a comma-separated list of positive counts")
	errIncomplete  = errors.New("exp-02: a drain did not see exactly the events this run produced")
	errUndecodable = errors.New("exp-02: undecodable event")
)

type settings struct {
	brokers   string
	topic     string
	events    int
	merchants int
	hotShare  int
	handler   time.Duration
	groups    []int
	timeout   time.Duration
}

type event struct {
	RunID      string `json:"run_id"`
	MerchantID string `json:"merchant_id"`
	Filler     string `json:"filler,omitempty"`
}

// drain is one measurement: how long a group of this size took, and how the work fell
// across its members.
type drain struct {
	Consumers   int
	Took        time.Duration
	PerConsumer []int
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-02 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		cfg    settings
		groups string
	)
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.topic, "topic", "exp02.hotkey", "topic to produce into and drain; the control cell uses its own")
	flag.IntVar(&cfg.events, "events", 20_000, "events to produce")
	flag.IntVar(&cfg.merchants, "merchants", 20, "merchants to spread the cold events over")
	flag.IntVar(&cfg.hotShare, "hot-share", 80, "percentage of events belonging to one merchant; 0 spreads every event over the merchants")
	flag.DurationVar(&cfg.handler, "handler", 200*time.Microsecond, "work per record, so drain time is about handling and not the network")
	flag.StringVar(&groups, "consumers", "1,2,3,6,7", "group sizes to measure, in order")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Minute, "deadline for the whole run")
	flag.Parse()

	sizes, err := groupSizes(groups)
	if err != nil {
		return err
	}
	cfg.groups = sizes
	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	return measure(ctx, &cfg, os.Stdout)
}

func (cfg *settings) validate() error {
	switch {
	case cfg.events <= 0 || cfg.events > maxEvents:
		return fmt.Errorf("%w: -events is %d, wanted 1 to %d", errShape, cfg.events, maxEvents)
	case cfg.merchants <= 1:
		return fmt.Errorf("%w: -merchants is %d, wanted at least 2", errShape, cfg.merchants)
	case cfg.hotShare < 0 || cfg.hotShare > 100:
		return fmt.Errorf("%w: got %d", errSkew, cfg.hotShare)
	}
	return nil
}

func groupSizes(list string) ([]int, error) {
	sizes := make([]int, 0, strings.Count(list, ",")+1)
	for field := range strings.SplitSeq(list, ",") {
		size, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || size <= 0 || size > maxConsumers {
			return nil, fmt.Errorf("%w: %q, wanted 1 to %d", errGroupSizes, field, maxConsumers)
		}
		sizes = append(sizes, size)
	}
	return sizes, nil
}

func measure(ctx context.Context, cfg *settings, out io.Writer) error {
	runID := fmt.Sprintf("exp02-%d", time.Now().UnixNano())

	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...))
	if err != nil {
		return fmt.Errorf("exp-02: kafka client: %w", err)
	}

	perPartition, err := produce(ctx, client, runID, cfg)
	client.Close()
	if err != nil {
		return err
	}

	// A drain that fails must not take the measurements already made with it: the table is
	// printed either way, and the failure is returned after it.
	drains := make([]drain, 0, len(cfg.groups))
	for _, size := range cfg.groups {
		measured, err := drainWith(ctx, cfg, runID, size)
		if err != nil {
			return errors.Join(report(out, cfg, perPartition, drains), err)
		}
		drains = append(drains, measured)
	}
	return report(out, cfg, perPartition, drains)
}

// produce sends -hot-share percent of the events under a single merchant key, so one
// partition ends up holding most of the topic.
func produce(ctx context.Context, client *kgo.Client, runID string, cfg *settings) (map[int32]int, error) {
	filler := strings.Repeat("x", fillerBytes)

	records := make([]*kgo.Record, 0, cfg.events)
	for i := range cfg.events {
		merchant := fmt.Sprintf("m-%02d", i%(cfg.merchants-1)+1)
		if i%100 < cfg.hotShare {
			merchant = "m-hot"
		}

		value, err := json.Marshal(event{
			RunID:      runID,
			MerchantID: merchant,
			Filler:     filler,
		})
		if err != nil {
			return nil, fmt.Errorf("exp-02: encode event %d: %w", i, err)
		}
		records = append(records, &kgo.Record{
			Topic: cfg.topic,
			Key:   []byte(merchant),
			Value: value,
		})
	}

	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return nil, fmt.Errorf("exp-02: produce %d events: %w", cfg.events, err)
	}

	perPartition := make(map[int32]int, len(records))
	for _, record := range records {
		perPartition[record.Partition]++
	}
	return perPartition, nil
}

// drainWith runs one consumer group of the given size over the same data and times it. The
// group name carries the run and the size, so every measurement starts from the beginning
// of the topic; offsets are committed as the work happens, so a partition that moves
// between members during the join resumes instead of being handled twice.
func drainWith(ctx context.Context, cfg *settings, runID string, consumers int) (drain, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	group := fmt.Sprintf("%s-k%d", runID, consumers)
	handled := make([]atomic.Int64, consumers)
	var (
		total    atomic.Int64
		finished atomic.Bool
	)

	started := time.Now()
	workers, workerCtx := errgroup.WithContext(ctx)
	for member := range consumers {
		workers.Go(func() error {
			return consume(workerCtx, &completion{
				done:    cancel,
				reached: &finished,
			}, cfg, runID, group, &handled[member], &total)
		})
	}
	if err := workers.Wait(); err != nil {
		return drain{}, err
	}

	took := time.Since(started)
	perConsumer := make([]int, consumers)
	for member := range consumers {
		perConsumer[member] = int(handled[member].Load())
	}
	if int(total.Load()) != cfg.events {
		return drain{}, fmt.Errorf("exp-02: %w: group of %d handled %d, produced %d", errIncomplete, consumers, total.Load(), cfg.events)
	}
	return drain{
		Consumers:   consumers,
		Took:        took,
		PerConsumer: perConsumer,
	}, nil
}

// consume is one member of the group. It stops when the whole group has handled every event
// of this run — the member that sees the last one cancels the context the others wait on.
// completion is how a member says "the group is done" without borrowing the meaning of a
// cancelled context: an interrupt and a finished measurement must not look alike.
type completion struct {
	done    context.CancelFunc
	reached *atomic.Bool
}

func consume(ctx context.Context, finish *completion, cfg *settings, runID, group string, handled, total *atomic.Int64) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(cfg.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("exp-02: kafka client for %s: %w", group, err)
	}
	defer leave(ctx, client)

	for ctx.Err() == nil {
		mine, err := poll(ctx, client, cfg, runID, group, finish.reached)
		if err != nil {
			return err
		}

		handled.Add(int64(mine))
		if total.Add(int64(mine)) >= int64(cfg.events) {
			finish.reached.Store(true)
			finish.done()
			return nil
		}
	}
	return nil
}

// leave bounds the shutdown: Close waits for the group to be left on the client's own
// internal context, which no deadline of this program reaches. Leaving first, under a
// deadline of ours, keeps an unresponsive broker from holding the run open.
func leave(ctx context.Context, client *kgo.Client) {
	leaveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaveTimeout)
	defer cancel()

	if err := client.LeaveGroupContext(leaveCtx); err != nil {
		slog.Warn("exp-02: leaving the group", "error", err)
	}
	client.Close()
}

func poll(ctx context.Context, client *kgo.Client, cfg *settings, runID, group string, reached *atomic.Bool) (int, error) {
	fetches := client.PollRecords(ctx, pollBatch)
	if err := fetches.Err0(); err != nil {
		if reached.Load() && errors.Is(err, context.Canceled) {
			return 0, nil
		}
		return 0, fmt.Errorf("exp-02: poll in %s: %w", group, err)
	}

	mine, err := work(ctx, fetches, runID, cfg.handler)
	if err != nil {
		return 0, err
	}

	// Committing as the work happens is what makes a mid-join reassignment resume rather
	// than re-read a partition from the start and count it twice.
	if err := client.CommitUncommittedOffsets(context.WithoutCancel(ctx)); err != nil {
		return 0, fmt.Errorf("exp-02: commit in %s: %w", group, err)
	}
	return mine, nil
}

// work is the handler: a fixed cost per record of this run, so the drain time measures
// handling capacity rather than how fast the network can deliver.
func work(ctx context.Context, fetches kgo.Fetches, runID string, cost time.Duration) (int, error) {
	mine := 0

	var failure error
	fetches.EachRecord(func(record *kgo.Record) {
		if failure != nil || ctx.Err() != nil {
			return
		}

		var decoded event
		if err := json.Unmarshal(record.Value, &decoded); err != nil {
			failure = fmt.Errorf("%w: partition %d offset %d: %w", errUndecodable, record.Partition, record.Offset, err)
			return
		}
		if decoded.RunID != runID {
			return
		}
		time.Sleep(cost)
		mine++
	})
	return mine, failure
}

func report(out io.Writer, cfg *settings, perPartition map[int32]int, drains []drain) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	hot := hottest(perPartition)

	lines := make([]string, 0, 5+len(drains))
	lines = append(lines,
		fmt.Sprintf("events\t%d over %d merchants, %d%% to one of them", cfg.events, cfg.merchants, cfg.hotShare),
		fmt.Sprintf("partitions\t%s", formatShares(distribution(perPartition))),
		fmt.Sprintf("hottest partition\tp%d with %d records (%.0f%%)", hot.Partition, hot.Records, hot.Percent),
		fmt.Sprintf("ceiling\t%.2f× — the most any group can gain over one consumer: all records ÷ the hottest partition", ceiling(cfg.events, hot.Records)),
		"",
		"consumers\ttime to drain\tvs one consumer\tidle members\tper consumer",
	)
	for _, measured := range drains {
		lines = append(lines, fmt.Sprintf("%d\t%s\t%.2f×\t%d\t%v",
			measured.Consumers, measured.Took.Round(10*time.Millisecond), speedup(drains[0], measured), idle(measured.PerConsumer), measured.PerConsumer))
	}

	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("exp-02: write report: %w", err)
		}
	}
	return table.Flush()
}
