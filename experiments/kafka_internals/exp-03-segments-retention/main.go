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
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"

	compactTopic   = "exp03.compact"
	retentionTopic = "exp03.retention"

	pollBatch = 500
	readStall = 10 * time.Second
	settle    = time.Second
	maxKeys   = 100_000
)

var (
	errShape       = errors.New("exp-03: key or update count out of range")
	errNoCleaning  = errors.New("exp-03: the log was never cleaned inside the deadline")
	errNoPartition = errors.New("exp-03: the topic reported no partition 0")
	errUndecodable = errors.New("exp-03: unreadable record key")
	errShortRead   = errors.New("exp-03: the log went quiet before its last offset")
	errIncomplete  = errors.New("exp-03: the log before cleaning does not hold what was produced")
	errNoConfig    = errors.New("exp-03: the topic does not report the config the report needs")
)

type settings struct {
	brokers    string
	keys       int
	updates    int
	tombstones int
	records    int
	timeout    time.Duration
}

// observation is one look at a log: what a reader finds, and where the log starts.
type observation struct {
	summary
	StartOffset int64
	EndOffset   int64
}

func main() {
	if err := run(); err != nil {
		slog.Error("exp-03 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg settings
	flag.StringVar(&cfg.brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.IntVar(&cfg.keys, "keys", 50, "keys in the compacted topic")
	flag.IntVar(&cfg.updates, "updates", 40, "updates written per key")
	flag.IntVar(&cfg.tombstones, "tombstones", 10, "keys deleted afterwards")
	flag.IntVar(&cfg.records, "records", 2000, "records written to the retention topic")
	flag.DurationVar(&cfg.timeout, "timeout", 5*time.Minute, "deadline for the whole run")
	flag.Parse()

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
	case cfg.keys <= 0 || cfg.keys > maxKeys:
		return fmt.Errorf("%w: -keys is %d, wanted 1 to %d", errShape, cfg.keys, maxKeys)
	// Divide rather than multiply: cfg.keys is already bounded by the case above, so no
	// operator-supplied magnitude is multiplied by another and the bound cannot wrap.
	case cfg.updates <= 0 || cfg.updates > maxKeys*10/cfg.keys:
		return fmt.Errorf("%w: -updates is %d", errShape, cfg.updates)
	case cfg.tombstones < 0 || cfg.tombstones > cfg.keys:
		return fmt.Errorf("%w: -tombstones is %d, wanted 0 to %d", errShape, cfg.tombstones, cfg.keys)
	case cfg.records <= 0 || cfg.records > maxKeys*10:
		return fmt.Errorf("%w: -records is %d", errShape, cfg.records)
	}
	return nil
}

func measure(ctx context.Context, cfg *settings, out io.Writer) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...))
	if err != nil {
		return fmt.Errorf("exp-03: kafka client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	compaction, err := compact(ctx, client, admin, cfg)
	if err != nil {
		return err
	}

	retention, err := expire(ctx, client, admin, cfg)
	if err != nil {
		return err
	}

	policy, err := setting(ctx, admin, compactTopic, "cleanup.policy")
	if err != nil {
		return err
	}
	kept, err := setting(ctx, admin, retentionTopic, "retention.ms")
	if err != nil {
		return err
	}
	return report(out, cfg, policy, kept, &compaction, &retention)
}

// compact writes many updates per key, deletes some of them, and waits for the cleaner to
// leave only what a reader still needs: the last value of every surviving key.
func compact(ctx context.Context, client *kgo.Client, admin *kadm.Client, cfg *settings) ([2]observation, error) {
	var stages [2]observation

	records := make([]*kgo.Record, 0, cfg.keys*cfg.updates+cfg.tombstones)
	for update := range cfg.updates {
		for key := range cfg.keys {
			records = append(records, &kgo.Record{
				Topic: compactTopic,
				Key:   []byte(fmt.Sprintf("state-%03d", key)),
				Value: []byte(fmt.Sprintf(`{"update":%d}`, update)),
			})
		}
	}
	for key := range cfg.tombstones {
		records = append(records, &kgo.Record{
			Topic: compactTopic,
			Key:   []byte(fmt.Sprintf("state-%03d", key)),
		})
	}

	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return stages, fmt.Errorf("exp-03: produce %d records: %w", len(records), err)
	}

	before, err := observe(ctx, admin, cfg, compactTopic)
	if err != nil {
		return stages, err
	}
	// Refuse to report a "before" the cleaner already touched. segment.ms is a second here,
	// so a pass can fire during the produce, and a reduced "before" would make the very
	// first check in awaitCleaning succeed — two mid-compaction readings, self-consistent
	// and wrong. exp-01 and exp-02 refuse a short read for the same reason.
	if err := whole(before, len(records), compactTopic); err != nil {
		return stages, err
	}

	after, err := awaitCleaning(ctx, client, admin, cfg, compactTopic, before)
	if err != nil {
		return stages, err
	}
	return [2]observation{before, after}, nil
}

// expire writes to a topic kept for seconds, then waits for the broker to drop what has
// aged out. Deletion works on whole segments, so the active one always survives.
func expire(ctx context.Context, client *kgo.Client, admin *kadm.Client, cfg *settings) ([2]observation, error) {
	var stages [2]observation

	records := make([]*kgo.Record, 0, cfg.records)
	for i := range cfg.records {
		records = append(records, &kgo.Record{
			Topic: retentionTopic,
			Key:   []byte(fmt.Sprintf("event-%05d", i)),
			Value: []byte(fmt.Sprintf(`{"seq":%d}`, i)),
		})
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return stages, fmt.Errorf("exp-03: produce %d records: %w", len(records), err)
	}

	before, err := observe(ctx, admin, cfg, retentionTopic)
	if err != nil {
		return stages, err
	}
	if err := whole(before, len(records), retentionTopic); err != nil {
		return stages, err
	}

	after, err := awaitCleaning(ctx, client, admin, cfg, retentionTopic, before)
	if err != nil {
		return stages, err
	}
	return [2]observation{before, after}, nil
}

// setting asks the broker what it is enforcing rather than trusting the YAML: change the
// topic config and the report follows, instead of quietly printing what we meant to set.
func setting(ctx context.Context, admin *kadm.Client, topic, key string) (string, error) {
	resources, err := admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return "", fmt.Errorf("exp-03: describe config of %s: %w", topic, err)
	}
	resource, err := resources.On(topic, nil)
	if err != nil {
		return "", fmt.Errorf("exp-03: config of %s: %w", topic, err)
	}

	for _, config := range resource.Configs {
		if config.Key == key {
			return config.MaybeValue(), nil
		}
	}
	return "", fmt.Errorf("%w: %s on %s", errNoConfig, key, topic)
}

func whole(before observation, produced int, topic string) error {
	if got := before.Records - before.Rolls; got != produced {
		return fmt.Errorf("%w: %s holds %d of the %d produced", errIncomplete, topic, got, produced)
	}
	return nil
}

// awaitCleaning rolls the active segment — nothing is ever cleaned while it is open — and
// then watches until the log actually changes, rather than sleeping for a guessed while.
func awaitCleaning(ctx context.Context, client *kgo.Client, admin *kadm.Client, cfg *settings, topic string, before observation) (observation, error) {
	for ctx.Err() == nil {
		roll := &kgo.Record{
			Topic: topic,
			Key:   []byte(rollKey),
			Value: []byte(`{"roll":true}`),
		}
		if err := client.ProduceSync(ctx, roll).FirstErr(); err != nil {
			return observation{}, fmt.Errorf("exp-03: roll the segment of %s: %w", topic, err)
		}

		select {
		case <-ctx.Done():
			return observation{}, fmt.Errorf("exp-03: %w on %s", errNoCleaning, topic)
		case <-time.After(settle):
		}

		now, err := observe(ctx, admin, cfg, topic)
		if err != nil {
			return observation{}, err
		}
		// Cleaning shows up as either fewer records than were written, or a log that no
		// longer starts where it did.
		if now.Records <= before.Records || now.StartOffset > before.StartOffset {
			return now, nil
		}
	}
	return observation{}, fmt.Errorf("exp-03: %w on %s", errNoCleaning, topic)
}

func observe(ctx context.Context, admin *kadm.Client, cfg *settings, topic string) (observation, error) {
	extent, err := labkit.Spans(ctx, admin, topic)
	if err != nil {
		return observation{}, fmt.Errorf("exp-03: %s: %w", topic, err)
	}
	span, ok := extent[0]
	if !ok {
		return observation{}, fmt.Errorf("%s: %w", topic, errNoPartition)
	}

	records, err := readAll(ctx, cfg, topic, span.Start, span.End)
	if err != nil {
		return observation{}, err
	}
	return observation{
		summary:     summarise(records),
		StartOffset: span.Start,
		EndOffset:   span.End,
	}, nil
}

// readAll reads the log from wherever it now starts until it has seen the last offset the
// broker reported. Stopping on silence instead would be a measurement that cannot fail: a
// broker stalling mid-read would look exactly like a shorter log, and the short count would
// be published as the result. The last offset is always readable — the active segment is
// never cleaned — so a compacted log's gaps do not prevent reaching it.
func readAll(ctx context.Context, cfg *settings, topic string, start, end int64) ([]record, error) {
	if start >= end {
		return nil, nil
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("exp-03: reader for %s: %w", topic, err)
	}
	defer client.Close()

	var records []record
	for {
		batch, last, err := readBatch(ctx, client, topic, end)
		if err != nil {
			return nil, err
		}
		records = append(records, batch...)
		if last >= end-1 {
			return records, nil
		}
	}
}

// readBatch returns the next batch and the highest offset in it. Going quiet before the
// log's last offset is an error, not an ending.
func readBatch(ctx context.Context, client *kgo.Client, topic string, end int64) ([]record, int64, error) {
	idle, cancel := context.WithTimeout(ctx, readStall)
	defer cancel()

	fetches := client.PollRecords(idle, pollBatch)
	if err := fetches.Err0(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, 0, fmt.Errorf("%w: %s, wanted up to %d", errShortRead, topic, end-1)
		}
		return nil, 0, fmt.Errorf("exp-03: read %s: %w", topic, err)
	}
	// Err0 only reports the first partition, which is enough to spot a cancel but would
	// hide a fetch error on any other partition if these topics ever gained one.
	if err := fetches.Err(); err != nil {
		return nil, 0, fmt.Errorf("exp-03: read %s: %w", topic, err)
	}

	batch := make([]record, 0, pollBatch)
	last := int64(-1)
	var failure error
	fetches.EachRecord(func(polled *kgo.Record) {
		if len(polled.Key) == 0 {
			failure = fmt.Errorf("%w: %s offset %d", errUndecodable, topic, polled.Offset)
			return
		}
		last = max(last, polled.Offset)
		batch = append(batch, record{
			Key:       string(polled.Key),
			Tombstone: polled.Value == nil,
		})
	})
	return batch, last, failure
}

func report(out io.Writer, cfg *settings, policy, kept string, compaction, retention *[2]observation) error {
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)

	lines := []string{
		fmt.Sprintf("compacted topic\t%d keys × %d updates, then %d of the keys deleted, cleanup.policy %s", cfg.keys, cfg.updates, cfg.tombstones, policy),
		fmt.Sprintf("  before cleaning\t%d records readable (plus %d segment-roll markers), %d live keys, %d tombstones, log starts at %d",
			compaction[0].Records-compaction[0].Rolls, compaction[0].Rolls, compaction[0].LiveKeys, compaction[0].Tombstones, compaction[0].StartOffset),
		fmt.Sprintf("  after cleaning\t%d records readable (plus %d segment-roll markers), %d live keys, %d tombstones, log starts at %d",
			compaction[1].Records-compaction[1].Rolls, compaction[1].Rolls, compaction[1].LiveKeys, compaction[1].Tombstones, compaction[1].StartOffset),
		"",
		fmt.Sprintf("retention topic\t%d records, retention.ms %s as the broker reports it", cfg.records, kept),
		fmt.Sprintf("  before cleaning\t%d records readable (plus %d segment-roll markers), log starts at %d, ends at %d",
			retention[0].Records-retention[0].Rolls, retention[0].Rolls, retention[0].StartOffset, retention[0].EndOffset),
		fmt.Sprintf("  after cleaning\t%d records readable (plus %d segment-roll markers), log starts at %d, ends at %d",
			retention[1].Records-retention[1].Rolls, retention[1].Rolls, retention[1].StartOffset, retention[1].EndOffset),
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(table, line); err != nil {
			return fmt.Errorf("exp-03: write report: %w", err)
		}
	}
	return table.Flush()
}
