//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	paymentv1 "github.com/DmytroLysenko1/Kafka-lab/gen/payment/v1"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/outboxwire"
	schemas "github.com/DmytroLysenko1/Kafka-lab/proto"
)

func brokers(t *testing.T) []string {
	t.Helper()
	seeds := os.Getenv("KAFKA_BROKERS")
	if seeds == "" {
		t.Fatal("KAFKA_BROKERS is empty: run these through `make test-integration`, which points them at the compose cluster")
	}
	return strings.Split(seeds, ",")
}

func registryURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	if url == "" {
		t.Fatal("SCHEMA_REGISTRY_URL is empty: run these through `make test-integration`")
	}
	return url
}

// Each run gets a topic of its own: the catalog topics belong to the experiments, and a
// test that published into payments.main would leave records behind for them to count.
func freshTopic(t *testing.T) string {
	t.Helper()
	topic := "payments.itest." + uuid.New().String()[:8]

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...))
	if err != nil {
		t.Fatalf("connect to the cluster: %v", err)
	}
	t.Cleanup(client.Close)

	admin := kadm.NewClient(client)
	if _, err := admin.CreateTopic(t.Context(), 3, 3, map[string]*string{}, topic); err != nil {
		t.Fatalf("create %s: %v", topic, err)
	}
	t.Cleanup(func() {
		if _, err := admin.DeleteTopics(context.WithoutCancel(t.Context()), topic); err != nil {
			t.Logf("delete %s: %v", topic, err)
		}
	})
	return topic
}

// record is an authorisation made an hour ago: recent enough for the consumer to count — it
// refuses one older than the inbox can deduplicate (retention.CanBeDeduplicated) — and clearly
// apart from the time the broker stamps on the record.
func record(t *testing.T, paymentID string, minor int64) outbox.Record {
	t.Helper()
	occurred := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	payload, err := json.Marshal(outboxwire.Payload{
		PaymentID:   paymentID,
		MerchantID:  "m-42",
		AmountMinor: minor,
		Currency:    "EUR",
		OccurredAt:  occurred,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return outbox.Record{
		ID:          minor,
		AggregateID: paymentID,
		EventType:   outboxwire.EventTypeAuthorized,
		Payload:     payload,
		OccurredAt:  occurred,
	}
}

// readBack consumes what the publisher wrote and decodes it the way a consumer would: by
// the schema id in the record itself, fetched from the registry rather than assumed.
func readBack(t *testing.T, topic string, want int) []*kgo.Record {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers(t)...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(client.Close)

	reading, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var read []*kgo.Record
	for len(read) < want {
		fetches := client.PollRecords(reading, want-len(read))
		if err := fetches.Err(); err != nil {
			t.Fatalf("poll: %v", err)
		}
		fetches.EachRecord(func(r *kgo.Record) { read = append(read, r) })
	}
	return read
}

func decoder(t *testing.T, topic string) *sr.Serde {
	t.Helper()
	registry, err := sr.NewClient(sr.URLs(registryURL(t)))
	if err != nil {
		t.Fatalf("registry client: %v", err)
	}
	registered, err := registry.LookupSchema(t.Context(), topic+"-value", sr.Schema{
		Schema: schemas.PaymentEventsV1,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		t.Fatalf("look the published schema up: %v", err)
	}

	serde := sr.NewSerde()
	serde.Register(registered.ID, &paymentv1.PaymentAuthorized{},
		sr.DecodeFn(func(b []byte, v any) error {
			message, isProto := v.(proto.Message)
			if !isProto {
				return errors.New("not a protobuf message")
			}
			return proto.Unmarshal(b, message)
		}),
		sr.Index(0),
	)
	return serde
}

func TestPublishedRecordsCanBeReadBackThroughTheRegistry(t *testing.T) {
	topic := freshTopic(t)
	publisher, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(publisher.Close)

	paymentID := uuid.New().String()
	sent := record(t, paymentID, 1999)
	if err := publisher.Publish(t.Context(), []outbox.Record{sent}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	read := readBack(t, topic, 1)
	var got paymentv1.PaymentAuthorized
	if err := decoder(t, topic).Decode(read[0].Value, &got); err != nil {
		t.Fatalf("decode as a consumer would: %v", err)
	}

	want := &paymentv1.PaymentAuthorized{
		PaymentId:   paymentID,
		MerchantId:  "m-42",
		AmountMinor: 1999,
		Currency:    "EUR",
	}
	if diff := cmp.Diff(want, &got, protocmp.Transform(), protocmp.IgnoreFields(&paymentv1.PaymentAuthorized{}, "occurred_at")); diff != "" {
		t.Errorf("decoded event mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(paymentID, string(read[0].Key)); diff != "" {
		t.Errorf("partition key mismatch (-want +got):\n%s", diff)
	}

	headers := map[string]string{}
	for _, header := range read[0].Headers {
		headers[header.Key] = string(header.Value)
	}
	wantHeaders := map[string]string{"event_id": strconv.FormatInt(1999, 10), "event_type": outboxwire.EventTypeAuthorized}
	if diff := cmp.Diff(wantHeaders, headers); diff != "" {
		t.Errorf("headers mismatch (-want +got):\n%s", diff)
	}

	// The two times are deliberately different things. The broker refuses a record stamped
	// more than log.message.timestamp.after.max.ms — an hour here — into the future, so the
	// log's clock must stay the log's: an event dated by a skewed clock would otherwise
	// block every record queued behind it.
	if !got.GetOccurredAt().AsTime().Equal(sent.OccurredAt) {
		t.Errorf("payload occurred_at = %s, want the business time %s", got.GetOccurredAt().AsTime(), sent.OccurredAt)
	}
	if since := time.Since(read[0].Timestamp).Abs(); since > time.Minute {
		t.Errorf("the record was stamped %s away from now; the log's timestamp must be the publication time", since)
	}
}

// The reason every record is keyed by its payment: two events of one payment must be read
// in the order they happened, and that only holds inside one partition.
func TestEventsOfOnePaymentLandInOnePartition(t *testing.T) {
	topic := freshTopic(t)
	publisher, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(publisher.Close)

	paymentID := uuid.New().String()
	records := []outbox.Record{record(t, paymentID, 1), record(t, paymentID, 2), record(t, paymentID, 3)}
	if err := publisher.Publish(t.Context(), records); err != nil {
		t.Fatalf("publish: %v", err)
	}

	read := readBack(t, topic, len(records))
	partitions := map[int32]bool{}
	for _, message := range read {
		partitions[message.Partition] = true
	}
	if diff := cmp.Diff(1, len(partitions)); diff != "" {
		t.Errorf("partitions the payment landed in (-want +got):\n%s", diff)
	}

	offsets := make([]int64, 0, len(read))
	for _, message := range read {
		offsets = append(offsets, message.Offset)
	}
	if !slices.IsSorted(offsets) {
		t.Errorf("the events of one payment were read out of the order they were produced in: offsets %v", offsets)
	}
}

// A relay that publishes a record twice is normal: the schema id in the second copy must
// still be the one the registry knows, not a second registration with a new id.
func TestRepublishingUsesTheSchemaAlreadyRegistered(t *testing.T) {
	topic := freshTopic(t)
	first, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("first publisher: %v", err)
	}
	t.Cleanup(first.Close)

	second, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("second publisher: %v", err)
	}
	t.Cleanup(second.Close)

	paymentID := uuid.New().String()
	if err := first.Publish(t.Context(), []outbox.Record{record(t, paymentID, 7)}); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if err := second.Publish(t.Context(), []outbox.Record{record(t, paymentID, 7)}); err != nil {
		t.Fatalf("republish: %v", err)
	}

	read := readBack(t, topic, 2)
	serde := decoder(t, topic)
	for _, message := range read {
		var got paymentv1.PaymentAuthorized
		if err := serde.Decode(message.Value, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if diff := cmp.Diff(paymentID, got.GetPaymentId()); diff != "" {
			t.Errorf("decoded payment id mismatch (-want +got):\n%s", diff)
		}
	}
}

// A registry left at its default would accept any next version of the schema — Apicurio's
// default is NONE — so the publisher declares BACKWARD for its subject before registering.
// An incompatible change then fails at registration, at the publisher's start, instead of
// reaching consumers that cannot read it.
func TestThePublisherHoldsItsSubjectToBackwardCompatibility(t *testing.T) {
	topic := freshTopic(t)
	publisher, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(publisher.Close)

	registry, err := sr.NewClient(sr.URLs(registryURL(t)))
	if err != nil {
		t.Fatalf("registry client: %v", err)
	}
	got := registry.Compatibility(t.Context(), topic+"-value")
	if got[0].Err != nil {
		t.Fatalf("read compatibility: %v", got[0].Err)
	}
	if diff := cmp.Diff(sr.CompatBackward, got[0].Level); diff != "" {
		t.Errorf("subject compatibility mismatch (-want +got):\n%s", diff)
	}
}

func TestPublishRefusesARecordItCannotEncode(t *testing.T) {
	topic := freshTopic(t)
	publisher, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(publisher.Close)

	poison := record(t, uuid.New().String(), 1)
	poison.EventType = "payment.invented"

	if err := publisher.Publish(t.Context(), []outbox.Record{poison}); !errors.Is(err, kafka.ErrEncode) {
		t.Fatalf("err = %v, want %v", err, kafka.ErrEncode)
	}
}
