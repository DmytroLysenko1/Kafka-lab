//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
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

const (
	// tierLateness is how late past its due time a tier may count a record. Measured: 40–70 ms
	// with the tier's short fetch wait, 2.0 s with franz-go's default 5 s one.
	tierLateness   = time.Second
	hotMerchant    = "m-hot"
	hotEventID     = 9001
	hotAmountMinor = 700
)

// One partition by default, so "behind the contended record" means behind it in the same
// partition — the case the chain exists for.
func freshTopicWith(t *testing.T, settings map[string]*string) string {
	t.Helper()
	return freshTopicOf(t, 1, settings)
}

func freshTopicOf(t *testing.T, partitions int32, settings map[string]*string) string {
	t.Helper()
	topic := "payments.itest." + uuid.New().String()[:8]

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...))
	if err != nil {
		t.Fatalf("connect to the cluster: %v", err)
	}
	t.Cleanup(client.Close)

	admin := kadm.NewClient(client)
	if _, err := admin.CreateTopic(t.Context(), partitions, 3, settings, topic); err != nil {
		t.Fatalf("create %s: %v", topic, err)
	}
	t.Cleanup(func() {
		if _, err := admin.DeleteTopics(context.WithoutCancel(t.Context()), topic); err != nil {
			t.Logf("delete %s: %v", topic, err)
		}
	})
	return topic
}

// A tier counts its delay from the moment the broker appended the record, as the catalog's
// retry topics do.
func freshTier(t *testing.T) string {
	t.Helper()
	return freshTierOf(t, 1)
}

func freshTierOf(t *testing.T, partitions int32) string {
	t.Helper()
	return freshTopicOf(t, partitions, map[string]*string{"message.timestamp.type": new("LogAppendTime")})
}

// holdMerchant is the other transaction, on the hot merchant: a report or a reconciliation that took the
// merchant's row and has not let go. release ends it; the cleanup ends it regardless.
func holdMerchant(t *testing.T, pool *pgxpool.Pool) (release func()) {
	t.Helper()
	merchant := hotMerchant
	if _, err := pool.Exec(t.Context(),
		"INSERT INTO merchant_totals (merchant_id, authorized_minor, updated_at) VALUES ($1, 0, now())", merchant); err != nil {
		t.Fatalf("create the merchant's row: %v", err)
	}
	holder, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin the holding transaction: %v", err)
	}
	if _, err := holder.Exec(t.Context(), "SELECT 1 FROM merchant_totals WHERE merchant_id = $1 FOR UPDATE", merchant); err != nil {
		t.Fatalf("lock the merchant's row: %v", err)
	}

	released := false
	release = func() {
		if released {
			return
		}
		released = true
		if err := holder.Rollback(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("release the merchant's row: %v", err)
		}
	}
	t.Cleanup(release)
	return release
}

func payment(t *testing.T, merchant string, eventID, minor int64) outbox.Record {
	t.Helper()
	paymentID := uuid.New().String()
	occurred := time.Date(2026, time.September, 25, 9, 0, 0, 0, time.UTC)
	payload, err := json.Marshal(outbox.AuthorizedPayload{
		PaymentID:   paymentID,
		MerchantID:  merchant,
		AmountMinor: minor,
		Currency:    "EUR",
		OccurredAt:  occurred,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return outbox.Record{ID: eventID, AggregateID: paymentID, EventType: outbox.EventTypeAuthorized, Payload: payload, OccurredAt: occurred}
}

func publishAll(t *testing.T, topic string, records ...outbox.Record) {
	t.Helper()
	publisher, err := kafka.NewPublisher(t.Context(), brokers(t), topic, registryURL(t))
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	defer publisher.Close()
	if err := publisher.Publish(t.Context(), records); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func headersOf(record *kgo.Record) map[string]string {
	byKey := map[string]string{}
	for _, h := range record.Headers {
		byKey[h.Key] = string(h.Value)
	}
	return byKey
}

func inboxRows(t *testing.T, pool *pgxpool.Pool, eventID int64) int {
	t.Helper()
	var rows int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM inbox WHERE event_id = $1", strconv.FormatInt(eventID, 10)).Scan(&rows); err != nil {
		t.Fatalf("count inbox rows: %v", err)
	}
	return rows
}

// Before the chain, one merchant whose row another transaction held stopped every payment
// behind it in the partition. Now the payments behind it are counted while the lock is
// still held, and the contended one waits in the first tier instead of in front of them.
func TestPaymentsBehindAContendedOneAreCountedWhileItsMerchantIsStillLocked(t *testing.T) {
	main := freshTopicWith(t, map[string]*string{})
	tier := freshTier(t)
	dlq := freshTopicWith(t, map[string]*string{})
	storage, pool := consumerStorage(t)
	release := holdMerchant(t, pool)
	defer release()

	publishAll(t, main,
		payment(t, hotMerchant, hotEventID, hotAmountMinor),
		payment(t, "m-42", 1, 100),
		payment(t, "m-42", 2, 200),
		payment(t, "m-42", 3, 300),
	)

	stage := stageOn(main)
	stage.Next = tier
	runConsumer(t, storage, stage, dlq)

	waitFor(t, "the payments behind the contended one to be counted", func() bool {
		return merchantTotal(t, pool, "m-42") == 600
	})

	moved := readBack(t, tier, 1)[0]
	want := map[string]string{
		"event_id":           strconv.Itoa(hotEventID),
		"retry_attempt":      "1",
		"retry_origin_topic": main,
	}
	got := headersOf(moved)
	for key, value := range want {
		if diff := cmp.Diff(value, got[key]); diff != "" {
			t.Errorf("header %s of the moved record (-want +got):\n%s", key, diff)
		}
	}
	if diff := cmp.Diff(int64(0), merchantTotal(t, pool, hotMerchant)); diff != "" {
		t.Errorf("the locked merchant's total moved while it was locked (-want +got):\n%s", diff)
	}
}

// A tier that handed the record over early would lose to the same lock again and spend an
// attempt of three doing it; one that never handed it over would lose the payment. The
// record is counted, once, no sooner than its delay after the broker appended it and not
// much later either.
func TestATierCountsTheContendedPaymentOnceItsDelayHasPassedAndTheLockIsGone(t *testing.T) {
	const delay = 3 * time.Second
	main := freshTopicWith(t, map[string]*string{})
	// Six partitions like the catalog's tiers, two led by each broker: besides the one the
	// record waits on, an idle one on the same broker whose fetch is held open meanwhile —
	// the fetch a resumed partition has to wait out before it is fetched itself.
	tier := freshTierOf(t, 6)
	dlq := freshTopicWith(t, map[string]*string{})
	storage, pool := consumerStorage(t)
	release := holdMerchant(t, pool)

	publishAll(t, main, payment(t, hotMerchant, hotEventID, hotAmountMinor))

	first := stageOn(main)
	first.Next = tier
	runConsumer(t, storage, first, dlq)
	appended := readBack(t, tier, 1)[0].Timestamp
	release()

	runConsumer(t, storage, kafka.Stage{Topic: tier, Group: "itest-tier-" + uuid.New().String()[:8], Tier: 1, Delay: delay}, dlq)

	waitFor(t, "the tier to count the payment", func() bool {
		return merchantTotal(t, pool, hotMerchant) == hotAmountMinor
	})

	var consumedAt time.Time
	if err := pool.QueryRow(t.Context(), "SELECT consumed_at FROM inbox WHERE event_id = $1",
		strconv.Itoa(hotEventID)).Scan(&consumedAt); err != nil {
		t.Fatalf("read when it was counted: %v", err)
	}
	if early := appended.Add(delay).Sub(consumedAt); early > 0 {
		t.Errorf("counted %s before its delay was up (appended %s, counted %s)", early, appended, consumedAt)
	}
	// Late is a failure too: a tier that resumes a partition on time but fetches it seconds
	// afterwards turns a 5 s tier into a 10 s one.
	if late := consumedAt.Sub(appended.Add(delay)); late > tierLateness {
		t.Errorf("counted %s after its delay was up, more than %s (appended %s, counted %s)", late, tierLateness, appended, consumedAt)
	}
	if diff := cmp.Diff(1, inboxRows(t, pool, hotEventID)); diff != "" {
		t.Errorf("inbox rows for the payment (-want +got):\n%s", diff)
	}
}

// Contention that outlasts the whole chain is not lost and not retried forever: it is
// archived as exhausted. Once the lock is gone, replaying it puts the payment on the
// merchant's total — and a replay repeated by mistake, by a new group that has no record
// of the first, still counts it once.
func TestAPaymentHeldPastEveryTierIsArchivedAndReplayingItTwiceCountsItOnce(t *testing.T) {
	main := freshTopicWith(t, map[string]*string{})
	tier := freshTier(t)
	dlq := freshTopicWith(t, map[string]*string{})
	storage, pool := consumerStorage(t)
	release := holdMerchant(t, pool)

	publishAll(t, main, payment(t, hotMerchant, hotEventID, hotAmountMinor))

	first := stageOn(main)
	first.Next = tier
	runConsumer(t, storage, first, dlq)
	last := kafka.Stage{Topic: tier, Group: "itest-tier-" + uuid.New().String()[:8], Tier: 1, Delay: time.Second}
	runConsumer(t, storage, last, dlq)

	letter := headersOf(readBack(t, dlq, 1)[0])
	for key, value := range map[string]string{"dlq_class": string(kafka.ClassExhausted), "retry_origin_topic": main, "dlq_origin_topic": tier} {
		if diff := cmp.Diff(value, letter[key]); diff != "" {
			t.Errorf("header %s of the dead letter (-want +got):\n%s", key, diff)
		}
	}
	release()

	detours, err := kafka.NewDetours(brokers(t), dlq, unobserved{})
	if err != nil {
		t.Fatalf("detours: %v", err)
	}
	t.Cleanup(detours.Close)

	replay := kafka.ReplayRequest{DeadLetterTopic: dlq, Class: kafka.ClassExhausted, Into: tier, Group: "itest-replay-" + uuid.New().String()[:8]}
	replayed := func(request kafka.ReplayRequest) kafka.ReplayReport {
		t.Helper()
		report, err := kafka.Replay(t.Context(), brokers(t), request, detours)
		if err != nil {
			t.Fatalf("replay by %s: %v", request.Group, err)
		}
		return report
	}

	if diff := cmp.Diff(kafka.ReplayReport{Replayed: 1}, replayed(replay)); diff != "" {
		t.Errorf("first replay (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(kafka.ReplayReport{}, replayed(replay)); diff != "" {
		t.Errorf("the same group replayed a letter it had already sent (-want +got):\n%s", diff)
	}
	again := replay
	again.Group = "itest-replay-" + uuid.New().String()[:8]
	if diff := cmp.Diff(kafka.ReplayReport{Replayed: 1}, replayed(again)); diff != "" {
		t.Errorf("replay by a second group (-want +got):\n%s", diff)
	}

	waitFor(t, "the replayed payment to be counted", func() bool {
		return merchantTotal(t, pool, hotMerchant) == hotAmountMinor
	})
	waitCommitted(t, last.Group, tier)
	if diff := cmp.Diff(1, inboxRows(t, pool, hotEventID)); diff != "" {
		t.Errorf("inbox rows after two replays (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(int64(hotAmountMinor), merchantTotal(t, pool, hotMerchant)); diff != "" {
		t.Errorf("total after two replays (-want +got):\n%s", diff)
	}
}

func committedOffset(t *testing.T, admin *kadm.Client, group, topic string) int64 {
	t.Helper()
	committed, err := admin.FetchOffsetsForTopics(t.Context(), group, topic)
	if err != nil || committed.Error() != nil {
		return -1
	}
	at, ok := committed.Lookup(topic, 0)
	if !ok {
		return -1
	}
	return at.At
}

// Fourteen payments of one locked merchant in one partition cost two seconds each, 28 s in
// a batch that holds the rebalance throughout. Held that long, a group whose rebalance
// timeout is shorter removes the member, and the member's late commit can move the offset
// back. The batch commits what it has done once its budget is spent and lets the
// rebalance through — the offset moves while copies are still being made.
func TestABatchOfContendedPaymentsCommitsBeforeItHasMovedThemAll(t *testing.T) {
	const contended = 14
	main := freshTopicWith(t, map[string]*string{})
	tier := freshTier(t)
	dlq := freshTopicWith(t, map[string]*string{})
	storage, pool := consumerStorage(t)
	release := holdMerchant(t, pool)
	defer release()

	held := make([]outbox.Record, 0, contended)
	for i := range int64(contended) {
		held = append(held, payment(t, hotMerchant, hotEventID+i, hotAmountMinor))
	}
	publishAll(t, main, held...)

	stage := stageOn(main)
	stage.Next = tier
	runConsumer(t, storage, stage, dlq)

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(client.Close)
	admin := kadm.NewClient(client)

	waitFor(t, "the first commit", func() bool { return committedOffset(t, admin, stage.Group, main) > 0 })
	ends, err := admin.ListEndOffsets(t.Context(), tier)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		t.Fatalf("end offsets of the tier: %v", err)
	}
	moved, _ := ends.Lookup(tier, 0)
	if moved.Offset >= contended {
		t.Errorf("the first commit came only after all %d payments were moved: the batch held the rebalance throughout", contended)
	}
}

// A payment the retry tier cannot store is not a payment the consumer may forget. The copy
// fails, the offset stays where it was, and the consumer stops rather than moving on — a
// restart meets the same payment again.
func TestAContendedPaymentTheTierCannotStoreKeepsItsOffset(t *testing.T) {
	main := freshTopicWith(t, map[string]*string{})
	dlq := freshTopicWith(t, map[string]*string{})
	storage, pool := consumerStorage(t)
	release := holdMerchant(t, pool)
	defer release()

	publishAll(t, main, payment(t, hotMerchant, hotEventID, hotAmountMinor), payment(t, "m-42", 1, 100))

	record := merchants.NewRecordAuthorized(postgres.NewInboxStore(storage), postgres.NewTotalsStore(storage), storage, time.Now)
	detours, err := kafka.NewDetours(brokers(t), dlq, unobserved{})
	if err != nil {
		t.Fatalf("detours: %v", err)
	}
	t.Cleanup(detours.Close)

	stage := stageOn(main)
	stage.Next = "payments.itest.never-created-" + uuid.New().String()[:8]
	consumer, err := kafka.NewConsumer(brokers(t), stage, record, detours, slog.New(slog.DiscardHandler), unobserved{})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	running, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	err = consumer.Run(running)
	if !errors.Is(err, kafka.ErrRetry) {
		t.Fatalf("Run returned %v, want the failed move to a retry tier", err)
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(client.Close)
	if at := committedOffset(t, kadm.NewClient(client), stage.Group, main); at > 0 {
		t.Errorf("committed offset %d: the payment nobody stored was marked done", at)
	}
	if diff := cmp.Diff(int64(0), merchantTotal(t, pool, "m-42")); diff != "" {
		t.Errorf("the payment behind the unstored one was counted past it (-want +got):\n%s", diff)
	}
}

// After an outage a tier holds a backlog of records that are long due, and a poll returns
// only part of it. A record that is not due yet, fetched in that same poll from another
// partition, must hold back its own partition and nothing else: paused as a whole, the tier
// would leave the rest of the backlog waiting out a delay none of it has left to serve.
func TestADueBacklogIsNotHeldBackByAFreshRecordOnAnotherPartition(t *testing.T) {
	const (
		delay   = 10 * time.Second
		backlog = 600
	)
	scratch := freshTopicWith(t, map[string]*string{})
	dlq := freshTopicWith(t, map[string]*string{})
	tier := "payments.itest." + uuid.New().String()[:8]
	storage, pool := consumerStorage(t)

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	t.Cleanup(client.Close)
	admin := kadm.NewClient(client)
	if _, err := admin.CreateTopic(t.Context(), 2, 3, map[string]*string{"message.timestamp.type": new("LogAppendTime")}, tier); err != nil {
		t.Fatalf("create %s: %v", tier, err)
	}
	t.Cleanup(func() {
		if _, err := admin.DeleteTopics(context.WithoutCancel(t.Context()), tier); err != nil {
			t.Logf("delete %s: %v", tier, err)
		}
	})

	records := make([]outbox.Record, 0, backlog+1)
	for i := range int64(backlog) {
		records = append(records, payment(t, "m-42", 1+i, 1))
	}
	records = append(records, payment(t, "m-late", hotEventID, 1))
	publishAll(t, scratch, records...)
	encoded := readBack(t, scratch, backlog+1)

	moveTo := func(partition int32, from []*kgo.Record) {
		t.Helper()
		for _, record := range from {
			copied := &kgo.Record{Topic: tier, Partition: partition, Key: record.Key, Value: record.Value, Headers: record.Headers}
			if err := client.ProduceSync(t.Context(), copied).FirstErr(); err != nil {
				t.Fatalf("produce into %s/%d: %v", tier, partition, err)
			}
		}
	}
	moveTo(1, encoded[:backlog])
	time.Sleep(delay) // the backlog has to be old for this test to mean anything; there is no clock to advance on a broker
	moveTo(0, encoded[backlog:])

	started := time.Now()
	runConsumer(t, storage, kafka.Stage{Topic: tier, Group: "itest-tier-" + uuid.New().String()[:8], Tier: 1, Delay: delay}, dlq)

	waitFor(t, "the due backlog to be counted", func() bool { return merchantTotal(t, pool, "m-42") == backlog })
	if took := time.Since(started); took > delay/2 {
		t.Errorf("the due backlog took %s: it waited behind a record due %s after it entered", took, delay)
	}
	if diff := cmp.Diff(int64(0), merchantTotal(t, pool, "m-late")); diff != "" {
		t.Errorf("the fresh record was handled before its delay (-want +got):\n%s", diff)
	}
}
