// exp-11 measures what one undecodable record costs, using the service's own consumer
// rather than a stand written for the occasion: the only difference between the two cells
// is whether that consumer has somewhere to put a record it cannot read.
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
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/experiments/labkit"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
)

var errUnknownPhase = errors.New("exp-11: phase must be produce or consume")

var errNoDeadLetterRoute = errors.New("exp-11: this consumer has nowhere to put a record it cannot read")

var errListing = errors.New("exp-11: the cluster did not answer what the count is read from")

// deadLetterSink is what the consumer needs and this experiment swaps: a real topic in one
// cell, a refusal in the other.
type deadLetterSink interface {
	Retry(ctx context.Context, record *kgo.Record, topic string, attempt int) error
	DeadLetter(ctx context.Context, record *kgo.Record, reason error, class kafka.Class) error
}

// refusingDeadLetters is the "we never configured one" case, and it is the interesting one:
// the consumer holds the offset rather than dropping the record, so the partition stops.
// Nothing in this experiment is contended, so a retry never reaches it; it refuses that too
// rather than pretend to have stored something.
type refusingDeadLetters struct{}

func (refusingDeadLetters) Retry(context.Context, *kgo.Record, string, int) error {
	return errNoDeadLetterRoute
}

func (refusingDeadLetters) DeadLetter(context.Context, *kgo.Record, error, kafka.Class) error {
	return errNoDeadLetterRoute
}

// lines writes the report and keeps the first error rather than checking eight of them:
// a report that failed halfway is one failure, not eight.
type lines struct {
	out io.Writer
	err error
}

func (l *lines) printf(format string, args ...any) {
	if l.err != nil {
		return
	}
	_, l.err = fmt.Fprintf(l.out, format, args...)
}

type settings struct {
	phase        string
	topic        string
	dlqTopic     string
	group        string
	merchant     string
	firstEventID int64
	records      int
	poisonAt     int
	budget       time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("exp-11 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var s settings
	flag.StringVar(&s.phase, "phase", "", "produce or consume")
	flag.StringVar(&s.topic, "topic", "", "topic to produce into or read from")
	flag.StringVar(&s.dlqTopic, "dlq-topic", "", "dead letter topic; empty means this consumer has no route")
	flag.StringVar(&s.group, "group", "", "consumer group")
	flag.StringVar(&s.merchant, "merchant", "", "merchant the records are addressed to, which is how this cell's total is told from the other's")
	flag.Int64Var(&s.firstEventID, "first-event-id", 1, "event ids start here; the cells must not overlap or the inbox would call one a duplicate of the other")
	flag.IntVar(&s.records, "records", 100, "good records to produce")
	flag.IntVar(&s.poisonAt, "poison-at", 10, "how many good records go in before the record nobody can decode")
	flag.DurationVar(&s.budget, "budget", 30*time.Second, "how long the consumer is given")
	flag.Parse()

	ctx := context.Background()
	switch s.phase {
	case "produce":
		return produce(ctx, &s, os.Stdout)
	case "consume":
		return consume(ctx, &s, os.Stdout)
	default:
		return fmt.Errorf("phase %q: %w", s.phase, errUnknownPhase)
	}
}

func brokers() []string {
	return strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
}

func produce(ctx context.Context, s *settings, out io.Writer) error {
	publisher, err := kafka.NewPublisher(ctx, brokers(), s.topic, os.Getenv("SCHEMA_REGISTRY_URL"))
	if err != nil {
		return err
	}
	defer publisher.Close()

	poison, err := kgo.NewClient(kgo.SeedBrokers(brokers()...), kgo.DefaultProduceTopic(s.topic), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return err
	}
	defer poison.Close()

	if err := produceAll(ctx, publisher, poison, s); err != nil {
		return err
	}

	written := &lines{out: out}
	written.printf("produced %d good records with one undecodable record after %d of them\n", s.records, s.poisonAt)
	return written.err
}

func produceAll(ctx context.Context, publisher *kafka.Publisher, poison *kgo.Client, s *settings) error {
	for i := range s.records {
		if i == s.poisonAt {
			if err := producePoison(ctx, poison, s); err != nil {
				return err
			}
		}
		good, err := goodRecord(s, i)
		if err != nil {
			return err
		}
		if err := publisher.Publish(ctx, []outbox.Record{good}); err != nil {
			return fmt.Errorf("produce good record %d: %w", i, err)
		}
	}
	return nil
}

// The record looks ordinary — right topic, right headers — until something tries to read
// it. That is how a poison pill arrives: not as a malformed request, but as bytes that
// passed every check anyone thought to make before storing them.
func producePoison(ctx context.Context, client *kgo.Client, s *settings) error {
	record := &kgo.Record{
		Key:   []byte("poison"),
		Value: []byte("this was never protobuf"),
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(s.merchant + "-poison")},
			{Key: "event_type", Value: []byte(outbox.EventTypeAuthorized)},
		},
	}
	if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce the poison record: %w", err)
	}
	return nil
}

func goodRecord(s *settings, i int) (outbox.Record, error) {
	payload, err := json.Marshal(outbox.AuthorizedPayload{
		PaymentID:   fmt.Sprintf("%s-%d", s.merchant, i),
		MerchantID:  s.merchant,
		AmountMinor: 1,
		Currency:    "EUR",
		OccurredAt:  time.Now().UTC(),
	})
	if err != nil {
		return outbox.Record{}, err
	}
	return outbox.Record{
		ID:          s.firstEventID + int64(i),
		AggregateID: fmt.Sprintf("%s-%d", s.merchant, i),
		EventType:   outbox.EventTypeAuthorized,
		Payload:     payload,
		OccurredAt:  time.Now().UTC(),
	}, nil
}

func consume(ctx context.Context, s *settings, out io.Writer) error {
	storage, err := postgres.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer storage.Close()

	dead, closeDead, err := deadLetters(s)
	if err != nil {
		return err
	}
	defer closeDead()

	record := merchants.NewRecordAuthorized(
		postgres.NewInboxStore(storage),
		postgres.NewTotalsStore(storage),
		storage,
		time.Now,
	)
	// A restart is a new client: labkit.Supervise explains why reusing one would measure
	// the harness instead of the service.
	newConsumer := func() (labkit.Runner, error) {
		return kafka.NewConsumer(brokers(), kafka.Stage{Topic: s.topic, Group: s.group}, record, dead, slog.New(slog.DiscardHandler), labkit.Unwatched{})
	}

	bounded, cancel := context.WithTimeout(ctx, s.budget)
	defer cancel()

	started := time.Now()
	restarts, drained, err := labkit.Supervise(bounded, newConsumer, func() bool { return handled(ctx, storage, s) >= int64(s.records) })
	if err != nil {
		return err
	}
	elapsed := time.Since(started)

	return report(ctx, out, storage, s, restarts, drained, elapsed)
}

func deadLetters(s *settings) (deadLetterSink, func(), error) {
	if s.dlqTopic == "" {
		return refusingDeadLetters{}, func() {}, nil
	}
	dead, err := kafka.NewDetours(brokers(), s.dlqTopic, labkit.Unwatched{})
	if err != nil {
		return nil, func() {}, err
	}
	return dead, dead.Close, nil
}

func handled(ctx context.Context, storage *postgres.Storage, s *settings) int64 {
	total, err := merchants.NewReadTotal(postgres.NewTotalsStore(storage)).Execute(ctx, s.merchant)
	if err != nil {
		return 0
	}
	return total
}

// measured is what the cluster says about the run, separate from how it is printed.
type measured struct {
	produced int64
	read     int64
	archived int64
}

func measure(ctx context.Context, s *settings) (measured, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
	if err != nil {
		return measured{}, err
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	ends, err := admin.ListEndOffsets(ctx, s.topic)
	if err != nil {
		return measured{}, err
	}
	// Each iterates partitions that failed to load as readily as ones that answered, and a
	// failed one carries offset -1. Summed unchecked, a listing that never worked reports a
	// topic nobody produced to and a group that read nothing — which is the shape cell A
	// exists to report, arriving from the instrument instead of from the poison pill.
	if err := ends.Error(); err != nil {
		return measured{}, fmt.Errorf("%w: end offsets of %s: %w", errListing, s.topic, err)
	}
	committed, err := admin.FetchOffsetsForTopics(ctx, s.group, s.topic)
	if err != nil {
		return measured{}, err
	}
	if err := committed.Error(); err != nil {
		return measured{}, fmt.Errorf("%w: committed offsets of %s: %w", errListing, s.group, err)
	}

	var result measured
	ends.Each(func(offset kadm.ListedOffset) { result.produced += offset.Offset })
	committed.Each(func(offset kadm.OffsetResponse) {
		if offset.At > 0 {
			result.read += offset.At
		}
	})

	if s.dlqTopic == "" {
		return result, nil
	}
	archived, err := admin.ListEndOffsets(ctx, s.dlqTopic)
	if err != nil {
		return measured{}, err
	}
	if err := archived.Error(); err != nil {
		return measured{}, fmt.Errorf("%w: end offsets of %s: %w", errListing, s.dlqTopic, err)
	}
	archived.Each(func(offset kadm.ListedOffset) { result.archived += offset.Offset })
	return result, nil
}

func report(ctx context.Context, out io.Writer, storage *postgres.Storage, s *settings, restarts int, drained bool, elapsed time.Duration) error {
	result, err := measure(ctx, s)
	if err != nil {
		return err
	}

	route := "none"
	if s.dlqTopic != "" {
		route = s.dlqTopic
	}

	written := &lines{out: out}
	written.printf("dead letter route: %s\n", route)
	written.printf("records in the topic: %d (%d good, 1 undecodable after %d of them)\n", result.produced, s.records, s.poisonAt)
	written.printf("payments counted: %d of %d\n", handled(ctx, storage, s), s.records)
	written.printf("records archived: %d\n", result.archived)
	written.printf("offsets committed: %d, still to read: %d\n", result.read, result.produced-result.read)
	written.printf("consumer restarts: %d\n", restarts)
	if drained {
		written.printf("drained in %s\n", elapsed.Round(time.Millisecond))
	} else {
		written.printf("never drained; gave up after %s\n", elapsed.Round(time.Millisecond))
	}
	return written.err
}
