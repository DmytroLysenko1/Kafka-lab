//go:build integration

package kafka_test

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
)

func consumerStorage(t *testing.T) (*postgres.Storage, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Fatal("DATABASE_URL is empty: run these through `make test-integration`")
	}

	storage, err := postgres.New(t.Context(), url)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(storage.Close)
	if err := storage.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatalf("open the inspecting pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(t.Context(), "TRUNCATE inbox, merchant_totals"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return storage, pool
}

// stageOn is a main-topic stage with a group of its own, so no run inherits another's offsets.
func stageOn(topic string) kafka.Stage {
	return kafka.Stage{Topic: topic, Group: "itest-" + uuid.New().String()[:8]}
}

// runConsumer starts one stage and guarantees it is stopped and joined before the test
// returns: a consumer still polling after its test would take records meant for the next.
func runConsumer(t *testing.T, storage *postgres.Storage, stage kafka.Stage, dlqTopic string) {
	t.Helper()

	record := merchants.NewRecordAuthorized(
		postgres.NewInboxStore(storage),
		postgres.NewTotalsStore(storage),
		storage,
		time.Now,
	)
	detours, err := kafka.NewDetours(brokers(t), dlqTopic, unobserved{})
	if err != nil {
		t.Fatalf("detours: %v", err)
	}
	consumer, err := kafka.NewConsumer(brokers(t), stage, record, detours, slog.New(slog.DiscardHandler), unobserved{})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	stopped := make(chan error, 1)
	go func() { stopped <- consumer.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Errorf("consumer on %s: %v", stage.Topic, err)
		}
		consumer.Close()
		detours.Close()
	})
}

// unobserved stands in for the metrics: these tests read the database and the topics.
type unobserved struct{}

func (unobserved) Handled(bool, time.Duration) {}
func (unobserved) Retried(string)              {}
func (unobserved) DeadLettered(string)         {}
func (unobserved) Failed(string)               {}

// waitCommitted waits until the group has committed past every record the topic held when
// asked: the only proof that a consumer has handled something it produced no row for.
func waitCommitted(t *testing.T, group, topic string) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	ends, err := admin.ListEndOffsets(t.Context(), topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		t.Fatalf("end offsets of %s: %v", topic, err)
	}
	waitFor(t, "group "+group+" to commit to the end of "+topic, func() bool {
		committed, err := admin.FetchOffsetsForTopics(t.Context(), group, topic)
		if err != nil || committed.Error() != nil {
			return false
		}
		reached := true
		ends.Each(func(end kadm.ListedOffset) {
			at, ok := committed.Lookup(end.Topic, end.Partition)
			reached = reached && (end.Offset == 0 || ok && at.At >= end.Offset)
		})
		return reached
	})
}

func merchantTotal(t *testing.T, pool *pgxpool.Pool, merchantID string) int64 {
	t.Helper()
	var total int64
	err := pool.QueryRow(t.Context(),
		"SELECT coalesce((SELECT authorized_minor FROM merchant_totals WHERE merchant_id = $1), 0)", merchantID).Scan(&total)
	if err != nil {
		t.Fatalf("read the total: %v", err)
	}
	return total
}

func waitFor(t *testing.T, what string, settled func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if settled() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// The whole reason the inbox exists: the relay republishes after a crash in the window
// between the broker's ack and its own commit, and the merchant must not be credited twice.
func TestTheSameEventDeliveredTwiceMovesTheTotalOnce(t *testing.T) {
	topic := freshTopic(t)
	dlqTopic := freshTopic(t)
	storage, pool := consumerStorage(t)

	publisher, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(publisher.Close)

	event := record(t, uuid.New().String(), 1999)
	for range 2 {
		if err := publisher.Publish(t.Context(), []outbox.Record{event}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	stage := stageOn(topic)
	runConsumer(t, storage, stage, dlqTopic)

	waitFor(t, "the merchant's total to be credited", func() bool {
		return merchantTotal(t, pool, "m-42") == 1999
	})

	// The second copy is already in the topic; once the group has committed past it, the
	// total is what it will stay.
	waitCommitted(t, stage.Group, topic)
	if diff := cmp.Diff(int64(1999), merchantTotal(t, pool, "m-42")); diff != "" {
		t.Errorf("total after the redelivery (-want +got):\n%s", diff)
	}

	var claimed int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM inbox").Scan(&claimed); err != nil {
		t.Fatalf("count the inbox: %v", err)
	}
	if diff := cmp.Diff(1, claimed); diff != "" {
		t.Errorf("inbox rows (-want +got):\n%s", diff)
	}
}

// A record nobody can decode must leave the partition, or every record behind it waits
// forever on one that will never succeed.
func TestARecordNobodyCanDecodeGoesToTheDeadLetterTopicAndTheGroupMovesOn(t *testing.T) {
	topic := freshTopic(t)
	dlqTopic := freshTopic(t)
	storage, pool := consumerStorage(t)

	poison, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...), kgo.DefaultProduceTopic(topic), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatalf("poison producer: %v", err)
	}
	t.Cleanup(poison.Close)

	merchantKey := uuid.New().String()
	if err := poison.ProduceSync(t.Context(), &kgo.Record{
		Key:   []byte(merchantKey),
		Value: []byte("this was never protobuf"),
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte("poison-1")},
			{Key: "event_type", Value: []byte(outbox.EventTypeAuthorized)},
		},
	}).FirstErr(); err != nil {
		t.Fatalf("produce the poison record: %v", err)
	}

	publisher, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(publisher.Close)
	if err := publisher.Publish(t.Context(), []outbox.Record{record(t, uuid.New().String(), 500)}); err != nil {
		t.Fatalf("publish the good record: %v", err)
	}

	runConsumer(t, storage, stageOn(topic), dlqTopic)

	waitFor(t, "the record behind the poison one to be handled", func() bool {
		return merchantTotal(t, pool, "m-42") == 500
	})

	archived := readBack(t, dlqTopic, 1)
	if diff := cmp.Diff("this was never protobuf", string(archived[0].Value)); diff != "" {
		t.Errorf("the dead letter must carry the bytes that arrived (-want +got):\n%s", diff)
	}

	headers := map[string]string{}
	for _, header := range archived[0].Headers {
		headers[header.Key] = string(header.Value)
	}
	if headers["dlq_reason"] == "" {
		t.Error("the dead letter carries no reason; whoever opens it has to guess")
	}
	if diff := cmp.Diff(topic, headers["dlq_origin_topic"]); diff != "" {
		t.Errorf("origin topic header (-want +got):\n%s", diff)
	}
}

// The schema is the last line of defence under the length check in the use case: whatever
// a producer puts in a header, the key it becomes is bounded.
func TestTheSchemaRefusesAnIdentifierNoRealMerchantWouldHave(t *testing.T) {
	_, pool := consumerStorage(t)

	_, err := pool.Exec(t.Context(),
		"INSERT INTO inbox (event_id, consumed_at) VALUES ($1, $2)", strings.Repeat("e", 65), time.Now().UTC())
	if err == nil {
		t.Error("the schema accepted an event id of 65 characters")
	}

	_, err = pool.Exec(t.Context(),
		"INSERT INTO merchant_totals (merchant_id, authorized_minor, updated_at) VALUES ($1, $2, $3)",
		strings.Repeat("m", 65), 1, time.Now().UTC())
	if err == nil {
		t.Error("the schema accepted a merchant id of 65 characters")
	}
}

// An event the consumer can decode but must never count — here, one with no id to
// deduplicate by — is just as poisonous as bytes it cannot read.
func TestAnEventTheConsumerRefusesIsArchivedRatherThanRetriedForever(t *testing.T) {
	topic := freshTopic(t)
	dlqTopic := freshTopic(t)
	storage, pool := consumerStorage(t)

	publisher, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(publisher.Close)

	// A valid event id, so the refusal can only be about the amount: zero authorises
	// nothing, and no number of redeliveries will make it authorise something.
	reserving := record(t, uuid.New().String(), 0)
	reserving.ID = 77
	if err := publisher.Publish(t.Context(), []outbox.Record{reserving}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	runConsumer(t, storage, stageOn(topic), dlqTopic)

	archived := readBack(t, dlqTopic, 1)
	headers := map[string]string{}
	for _, header := range archived[0].Headers {
		headers[header.Key] = string(header.Value)
	}
	if headers["dlq_reason"] == "" {
		t.Error("the dead letter carries no reason")
	}
	if diff := cmp.Diff(int64(0), merchantTotal(t, pool, "m-42")); diff != "" {
		t.Errorf("a refused event still moved the total (-want +got):\n%s", diff)
	}
}
