// exp-12 asks two questions about schema evolution and answers both with runs: which
// changes the registry will let through at which setting, and what each of those changes
// actually does to a consumer built against the schema before it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	schemas "github.com/DmytroLysenko1/Kafka-lab/proto"
)

var errUnknownPhase = errors.New("exp-12: phase must be registry, produce or consume")

// lines writes a report and keeps the first error rather than checking every write.
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

const (
	fieldPaymentID  = 1
	fieldMerchantID = 2
	fieldAmount     = 3
	fieldCurrency   = 4
	// Six, not five: five is occurred_at in the schema the consumer knows, and writing a
	// string there would be a reused number rather than a new field — a different
	// experiment, and the one this cell is supposed to be contrasted against.
	fieldAddedLater = 6
)

// variant is one generation of the schema, written as bytes rather than generated code:
// four Go packages would prove that four packages compile, not what the bytes do to a
// reader that knows only the first of them.
type variant struct {
	name    string
	eventID string
	body    func(paymentID, merchant string) []byte
}

var variants = []variant{
	{
		name:    "v1, the schema the consumer was built against",
		eventID: "exp12-v1",
		body: func(paymentID, merchant string) []byte {
			body := appendString(nil, fieldPaymentID, paymentID)
			body = appendString(body, fieldMerchantID, merchant)
			body = protowire.AppendTag(body, fieldAmount, protowire.VarintType)
			body = protowire.AppendVarint(body, 1999)
			return appendString(body, fieldCurrency, "EUR")
		},
	},
	{
		name:    "a field added at the end",
		eventID: "exp12-added",
		body: func(paymentID, merchant string) []byte {
			body := appendString(nil, fieldPaymentID, paymentID)
			body = appendString(body, fieldMerchantID, merchant)
			body = protowire.AppendTag(body, fieldAmount, protowire.VarintType)
			body = protowire.AppendVarint(body, 1999)
			body = appendString(body, fieldCurrency, "EUR")
			return appendString(body, fieldAddedLater, "order-7788")
		},
	},
	{
		name:    "the amount removed",
		eventID: "exp12-removed",
		body: func(paymentID, merchant string) []byte {
			body := appendString(nil, fieldPaymentID, paymentID)
			body = appendString(body, fieldMerchantID, merchant)
			return appendString(body, fieldCurrency, "EUR")
		},
	},
	{
		name:    "the amount retyped to a string, keeping its number",
		eventID: "exp12-retyped",
		body: func(paymentID, merchant string) []byte {
			body := appendString(nil, fieldPaymentID, paymentID)
			body = appendString(body, fieldMerchantID, merchant)
			body = appendString(body, fieldAmount, "1999")
			return appendString(body, fieldCurrency, "EUR")
		},
	},
}

func appendString(body []byte, field protowire.Number, value string) []byte {
	body = protowire.AppendTag(body, field, protowire.BytesType)
	return protowire.AppendString(body, value)
}

type settings struct {
	phase       string
	topic       string
	dlqTopic    string
	group       string
	merchant    string
	registryURL string
	budget      time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("exp-12 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var s settings
	flag.StringVar(&s.phase, "phase", "", "registry, produce or consume")
	flag.StringVar(&s.topic, "topic", "exp12.evolution", "topic to write to or read from")
	flag.StringVar(&s.dlqTopic, "dlq-topic", "exp12.evolution.dlq", "dead letter topic")
	flag.StringVar(&s.group, "group", "exp12", "consumer group")
	flag.StringVar(&s.merchant, "merchant", "exp12", "merchant the records are addressed to")
	flag.DurationVar(&s.budget, "budget", 20*time.Second, "how long the consumer is given")
	flag.Parse()
	s.registryURL = os.Getenv("SCHEMA_REGISTRY_URL")

	ctx := context.Background()
	switch s.phase {
	case "registry":
		return compatibility(ctx, s.registryURL, os.Stdout)
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

// produce writes each variant with a valid Confluent header carrying the id of the schema
// the service publishes under: every record is well formed as far as the transport and the
// registry are concerned, which is the point — the damage is inside the payload.
func produce(ctx context.Context, s *settings, out io.Writer) error {
	registry, err := sr.NewClient(sr.URLs(s.registryURL))
	if err != nil {
		return err
	}
	registered, err := registry.CreateSchema(ctx, s.topic+"-value", sr.Schema{
		Schema: schemas.PaymentEventsV1,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		return err
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.DefaultProduceTopic(s.topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	header := sr.ConfluentHeader{}
	written := &lines{
		out: out,
	}

	for _, generation := range variants {
		prefix, err := header.AppendEncode(nil, registered.ID, []int{0})
		if err != nil {
			return err
		}
		paymentID := generation.eventID + "-payment"
		record := &kgo.Record{
			Key:   []byte(paymentID),
			Value: append(prefix, generation.body(paymentID, s.merchant)...),
			Headers: []kgo.RecordHeader{
				{Key: "event_id", Value: []byte(generation.eventID)},
				{Key: "event_type", Value: []byte(outbox.EventTypeAuthorized)},
			},
		}
		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
			return fmt.Errorf("produce %s: %w", generation.name, err)
		}
		written.printf("produced: %s\n", generation.name)
	}
	return written.err
}

func consume(ctx context.Context, s *settings, out io.Writer) error {
	storage, err := postgres.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer storage.Close()

	dead, err := kafka.NewDetours(brokers(), s.dlqTopic, metrics.Discard{})
	if err != nil {
		return err
	}
	defer dead.Close()

	record := merchants.NewRecordAuthorized(
		postgres.NewInboxStore(storage),
		postgres.NewTotalsStore(storage),
		storage,
		time.Now,
	)
	consumer, err := kafka.NewConsumer(brokers(), kafka.Stage{
		Topic: s.topic,
		Group: s.group,
	}, record, dead, slog.New(slog.DiscardHandler), metrics.Discard{})
	if err != nil {
		return err
	}
	defer consumer.Close()

	// The budget stops the consumer by cancelling it, not by giving it a deadline: a
	// deadline would also cut short the commit the consumer runs on its own timeout, and
	// the consumer is right to report that as a failure rather than as a clean stop.
	running, stop := context.WithCancel(ctx)
	defer stop()
	budget := time.AfterFunc(s.budget, stop)
	defer budget.Stop()

	if err := consumer.Run(running); err != nil {
		return err
	}

	return report(ctx, s, storage, out)
}

func report(ctx context.Context, s *settings, storage *postgres.Storage, out io.Writer) error {
	counted, err := countedEvents(ctx, storage)
	if err != nil {
		return err
	}
	archived, err := archivedReasons(ctx, s)
	if err != nil {
		return err
	}

	written := &lines{
		out: out,
	}
	written.printf("%-46s %s\n", "record", "what the v1 consumer did with it")
	for _, generation := range variants {
		switch {
		case counted[generation.eventID]:
			written.printf("%-46s counted\n", generation.name)
		case archived[generation.eventID] != "":
			written.printf("%-46s dead letter: %s\n", generation.name, archived[generation.eventID])
		default:
			written.printf("%-46s neither counted nor archived\n", generation.name)
		}
	}

	total, err := merchants.NewReadTotal(postgres.NewTotalsStore(storage)).Execute(ctx, s.merchant)
	if err != nil {
		return err
	}
	written.printf("\nmerchant total: %d minor units from %d records\n", total, len(variants))
	return written.err
}

func countedEvents(ctx context.Context, storage *postgres.Storage) (map[string]bool, error) {
	counted := make(map[string]bool)
	for _, generation := range variants {
		claimed, err := alreadyClaimed(ctx, storage, generation.eventID)
		if err != nil {
			return nil, err
		}
		counted[generation.eventID] = claimed
	}
	return counted, nil
}

// alreadyClaimed asks the inbox whether this event was handled, by trying to claim it in a
// transaction that is then rolled back: a claim that fails is a claim somebody else made.
func alreadyClaimed(ctx context.Context, storage *postgres.Storage, eventID string) (bool, error) {
	inbox := postgres.NewInboxStore(storage)
	claimed := false

	err := storage.WithinTx(ctx, func(ctx context.Context) error {
		first, err := inbox.Claim(ctx, eventID, time.Now())
		if err != nil {
			return err
		}
		claimed = !first
		return errRollback
	})
	if err != nil && !errors.Is(err, errRollback) {
		return false, err
	}
	return claimed, nil
}

var errRollback = errors.New("exp-12: rolled back on purpose")

func archivedReasons(ctx context.Context, s *settings) (map[string]string, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumeTopics(s.dlqTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	reading, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	reasons := make(map[string]string)
	for len(reasons) < len(variants) {
		fetches := client.PollRecords(reading, len(variants))
		if fetches.Err() != nil {
			break
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if eventID, reason := archivedRecord(record); eventID != "" {
				reasons[eventID] = reason
			}
		})
	}
	return reasons, nil
}

func archivedRecord(record *kgo.Record) (eventID, reason string) {
	for _, header := range record.Headers {
		switch header.Key {
		case "event_id":
			eventID = string(header.Value)
		case "dlq_reason":
			reason = string(header.Value)
		}
	}
	return eventID, reason
}
