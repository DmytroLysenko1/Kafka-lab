package main

import (
	"cmp"
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

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	defaultTimeout = 30 * time.Second

	maxPartitions        = 1000
	maxReplicationFactor = 100

	usageText = `usage: labctl [flags] <command> [arguments]

commands:
  topics                     create the lab topic catalog, or report how a live topic drifted from it
  create-topic -name NAME    create one experiment topic (-partitions, -rf, repeatable -config key=value)
  delete-topic TOPIC ...     delete topics an experiment reshaped, so the catalog can be recreated
  add-partitions -add N T    add N partitions to topics T (the exp-02 remedy; it rekeys existing keys)
  describe [topic ...]       leader, replicas, ISR, under-replication and load status per partition
  lag GROUP ...              committed offset, end offset and lag per partition of a consumer group

flags:
`
)

var (
	errUsage          = errors.New("labctl: no command given")
	errUnknownCommand = errors.New("labctl: unknown command")
	errTopicName      = errors.New("labctl: -name is required")
	errTopicShape     = errors.New("labctl: topic shape out of range")
	errTopicConfig    = errors.New("labctl: -config expects key=value")
	errNoTopics       = errors.New("labctl: name at least one topic")
	errNoGroups       = errors.New("labctl: name at least one consumer group")
	errPartitionCount = errors.New("labctl: -add must be a positive partition count")
	errBrokerList     = errors.New("labctl: -brokers has an empty entry")
)

type command func(ctx context.Context, admin *kafka.Admin, out io.Writer, args []string) error

func commands() map[string]command {
	return map[string]command{
		"topics":         ensureCatalog,
		"create-topic":   createTopic,
		"delete-topic":   deleteTopics,
		"add-partitions": addPartitions,
		"describe":       describeTopics,
		"lag":            describeLag,
	}
}

func main() {
	err := run()
	switch {
	case err == nil:
	case errors.Is(err, errUsage), errors.Is(err, errUnknownCommand):
		os.Exit(2)
	default:
		slog.Error("labctl failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	brokers := flag.String("brokers", cmp.Or(os.Getenv("KAFKA_BROKERS"), defaultBrokers), "comma-separated bootstrap brokers")
	timeout := flag.Duration("timeout", defaultTimeout, "deadline for the whole command")
	verbose := flag.Bool("v", false, "log the client's broker traffic to stderr")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		return writeUsage(errUsage)
	}
	execute, ok := commands()[args[0]]
	if !ok {
		return writeUsage(fmt.Errorf("%w %q", errUnknownCommand, args[0]))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	seeds, err := parseBrokers(*brokers)
	if err != nil {
		return err
	}

	admin, err := kafka.NewAdmin(seeds, adminOptions(*verbose)...)
	if err != nil {
		return err
	}
	defer admin.Close()

	return execute(ctx, admin, os.Stdout, args[1:])
}

func ensureCatalog(ctx context.Context, admin *kafka.Admin, out io.Writer, _ []string) error {
	statuses, ensureErr := admin.EnsureTopics(ctx, kafka.LabTopics())
	if err := renderTopics(out, statuses); err != nil {
		return err
	}
	return ensureErr
}

func createTopic(ctx context.Context, admin *kafka.Admin, out io.Writer, args []string) error {
	settings := configFlag{}
	flags := flag.NewFlagSet("create-topic", flag.ContinueOnError)
	name := flags.String("name", "", "topic name")
	partitions := flags.Int("partitions", 1, "partition count")
	replication := flags.Int("rf", 3, "replication factor")
	flags.Var(settings, "config", "topic config as key=value; repeat the flag for more than one")
	if err := flags.Parse(args); err != nil {
		return err
	}

	spec, err := topicSpec(*name, *partitions, *replication, settings)
	if err != nil {
		return err
	}

	statuses, ensureErr := admin.EnsureTopics(ctx, []kafka.TopicSpec{spec})
	if err := renderTopics(out, statuses); err != nil {
		return err
	}
	return ensureErr
}

func deleteTopics(ctx context.Context, admin *kafka.Admin, _ io.Writer, topics []string) error {
	if len(topics) == 0 {
		return errNoTopics
	}
	return admin.DeleteTopics(ctx, topics...)
}

func addPartitions(ctx context.Context, admin *kafka.Admin, out io.Writer, args []string) error {
	flags := flag.NewFlagSet("add-partitions", flag.ContinueOnError)
	add := flags.Int("add", 0, "how many partitions to add to each topic")
	if err := flags.Parse(args); err != nil {
		return err
	}

	topics := flags.Args()
	switch {
	case *add < 1 || *add > maxPartitions:
		return fmt.Errorf("%w: %d", errPartitionCount, *add)
	case len(topics) == 0:
		return errNoTopics
	}

	if err := admin.AddPartitions(ctx, *add, topics...); err != nil {
		return err
	}
	return describeTopics(ctx, admin, out, topics)
}

func describeTopics(ctx context.Context, admin *kafka.Admin, out io.Writer, topics []string) error {
	if len(topics) == 0 {
		topics = kafka.LabTopicNames()
	}

	states, err := admin.Describe(ctx, topics...)
	if err != nil {
		return err
	}
	return renderPartitions(out, states)
}

func describeLag(ctx context.Context, admin *kafka.Admin, out io.Writer, groups []string) error {
	if len(groups) == 0 {
		return errNoGroups
	}

	lags, lagErr := admin.Lag(ctx, groups...)
	if err := renderLag(out, lags); err != nil {
		return err
	}
	return lagErr
}

type configFlag map[string]string

func (c configFlag) String() string {
	return strings.Join(lo.MapToSlice(c, func(key, value string) string {
		return key + "=" + value
	}), ",")
}

func (c configFlag) Set(raw string) error {
	key, value, ok := strings.Cut(raw, "=")
	if !ok || key == "" {
		return fmt.Errorf("%w: %q", errTopicConfig, raw)
	}
	c[key] = value
	return nil
}

func topicSpec(name string, partitions, replication int, settings configFlag) (kafka.TopicSpec, error) {
	if name == "" {
		return kafka.TopicSpec{}, errTopicName
	}
	if partitions < 1 || partitions > maxPartitions || replication < 1 || replication > maxReplicationFactor {
		return kafka.TopicSpec{}, fmt.Errorf("%w: partitions=%d rf=%d", errTopicShape, partitions, replication)
	}

	config := map[string]string(settings)
	if len(config) == 0 {
		config = nil
	}
	return kafka.TopicSpec{
		Name:              name,
		Partitions:        int32(partitions),
		ReplicationFactor: int16(replication),
		Config:            config,
	}, nil
}

func parseBrokers(raw string) ([]string, error) {
	seeds := lo.Map(strings.Split(raw, ","), func(seed string, _ int) string {
		return strings.TrimSpace(seed)
	})
	if slices.Contains(seeds, "") {
		return nil, fmt.Errorf("%w: %q", errBrokerList, raw)
	}
	return seeds, nil
}

func adminOptions(verbose bool) []kafka.Option {
	if !verbose {
		return nil
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return []kafka.Option{kafka.WithLogger(logger)}
}

func writeUsage(cause error) error {
	if _, err := io.WriteString(flag.CommandLine.Output(), usageText); err != nil {
		return err
	}
	flag.PrintDefaults()
	return cause
}
