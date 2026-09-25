package kafka

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"
)

var entered = time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

// A tier that handed a record over early would retry against the same lock it just lost
// to, and burn an attempt of three doing it. A main topic that waited would hold every
// payment for a clock skew between two machines.
func TestARecordIsDueNoSoonerThanItsTierDelayAfterItEnteredTheTier(t *testing.T) {
	type args struct {
		delay time.Duration
		now   time.Time
	}
	tests := []struct {
		name    string
		args    args
		want    time.Duration
		wantErr error
	}{
		{name: "just entered a five-second tier: waits all of it", args: args{delay: 5 * time.Second, now: entered}, want: 5 * time.Second},
		{name: "two seconds into it: waits the rest", args: args{delay: 5 * time.Second, now: entered.Add(2 * time.Second)}, want: 3 * time.Second},
		{name: "exactly at the deadline: due", args: args{delay: 5 * time.Second, now: entered.Add(5 * time.Second)}, want: 0},
		{name: "long past it, after an outage: due, not negative", args: args{delay: 5 * time.Second, now: entered.Add(time.Hour)}, want: 0},
		{name: "main topic, producer clock a minute ahead: never waits", args: args{delay: 0, now: entered.Add(-time.Minute)}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stage := Stage{Topic: "tier", Delay: tt.args.delay}
			got := stage.untilDue(&kgo.Record{Timestamp: entered}, tt.args.now)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("wait mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// What an operator reads off a dead letter after three tiers: one attempt count, the
// position where the payment was first read, and the event id that deduplicates it.
func TestARecordCarriesOneAttemptCountAndItsFirstOriginThroughTheChain(t *testing.T) {
	fromMain := &kgo.Record{
		Topic: "payments.main", Partition: 3, Offset: 41,
		Headers: []kgo.RecordHeader{{Key: "event_id", Value: []byte("7")}},
	}
	intoFirstTier := &kgo.Record{Topic: "retry.5s", Partition: 3, Offset: 2, Headers: retryHeaders(fromMain, 1, entered)}
	intoSecondTier := &kgo.Record{Topic: "retry.1m", Partition: 3, Offset: 0, Headers: retryHeaders(intoFirstTier, 2, entered.Add(5*time.Second))}
	moved := retryHeaders(intoSecondTier, 3, entered.Add(time.Minute))

	want := map[string]string{
		"event_id":               "7",
		"retry_attempt":          "3",
		"retry_origin_topic":     "payments.main",
		"retry_origin_partition": "3",
		"retry_origin_offset":    "41",
		"retry_first_failed_at":  "2026-09-25T12:00:00Z",
	}
	if diff := cmp.Diff(want, headerMap(moved)); diff != "" {
		t.Errorf("headers after three hops (-want +got):\n%s", diff)
	}
	if len(moved) != len(want) {
		t.Errorf("a header was appended twice: %d headers, want %d", len(moved), len(want))
	}
}

// A producer can put any header on the main topic, including ours. Kept, a forged origin
// would send whoever investigates a dead letter to a partition and offset of the forger's
// choosing, and a forged attempt count would say the payment had been tried when it had not.
func TestRetryHeadersAProducerForgedAreReplacedOnTheFirstAttempt(t *testing.T) {
	forged := &kgo.Record{
		Topic: "payments.main", Partition: 1, Offset: 9,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte("7")},
			{Key: "retry_attempt", Value: []byte("7")},
			{Key: "retry_origin_topic", Value: []byte("somewhere.else")},
			{Key: "retry_origin_partition", Value: []byte("0")},
			{Key: "retry_origin_offset", Value: []byte("123456")},
			{Key: "retry_first_failed_at", Value: []byte("1999-01-01T00:00:00Z")},
		},
	}

	moved := retryHeaders(forged, 1, entered)

	want := map[string]string{
		"event_id":               "7",
		"retry_attempt":          "1",
		"retry_origin_topic":     "payments.main",
		"retry_origin_partition": "1",
		"retry_origin_offset":    "9",
		"retry_first_failed_at":  "2026-09-25T12:00:00Z",
	}
	if diff := cmp.Diff(want, headerMap(moved)); diff != "" {
		t.Errorf("headers after the first attempt (-want +got):\n%s", diff)
	}
	if len(moved) != len(want) {
		t.Errorf("a forged header survived beside ours: %d headers, want %d", len(moved), len(want))
	}
}

// A reader takes the first header of a name. A dead letter that kept a producer's own
// dlq_reason ahead of ours would tell the operator whatever the producer wanted.
func TestADeadLetterCarriesOnlyItsOwnVerdict(t *testing.T) {
	forged := &kgo.Record{
		Topic: "payments.main", Partition: 2, Offset: 5,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte("7")},
			{Key: "dlq_reason", Value: []byte("nothing to see")},
			{Key: "dlq_origin_topic", Value: []byte("somewhere.else")},
			{Key: "dlq_class", Value: []byte("exhausted")},
		},
	}

	letter := deadLetterHeaders(forged, errors.New("merchants: the event names no merchant"), ClassRefused, entered)

	want := map[string]string{
		"event_id":             "7",
		"dlq_class":            "refused",
		"dlq_reason":           "merchants: the event names no merchant",
		"dlq_origin_topic":     "payments.main",
		"dlq_origin_partition": "2",
		"dlq_origin_offset":    "5",
		"dlq_archived_at":      "2026-09-25T12:00:00Z",
	}
	if diff := cmp.Diff(want, headerMap(letter)); diff != "" {
		t.Errorf("dead letter headers (-want +got):\n%s", diff)
	}
	if len(letter) != len(want) {
		t.Errorf("a forged header survived beside ours: %d headers, want %d", len(letter), len(want))
	}
}

// The fetched record belongs to the client's buffers; writing a header into its backing
// array would change a record another part of the batch may still be reading.
func TestMovingARecordOnLeavesTheFetchedRecordUntouched(t *testing.T) {
	headers := make([]kgo.RecordHeader, 1, 8)
	headers[0] = kgo.RecordHeader{Key: "event_id", Value: []byte("7")}
	fetched := &kgo.Record{Topic: "payments.main", Headers: headers}

	_ = retryHeaders(fetched, 1, entered)
	_ = deadLetterHeaders(fetched, errors.New("refused"), ClassRefused, entered)
	_ = replayHeaders(fetched)

	want := []kgo.RecordHeader{{Key: "event_id", Value: []byte("7")}}
	if diff := cmp.Diff(want, fetched.Headers); diff != "" {
		t.Errorf("fetched headers changed (-want +got):\n%s", diff)
	}
	if spare := headers[:2][1]; spare.Key != "" {
		t.Errorf("the spare capacity of the fetched slice was written: %+v", spare)
	}
}

// A replayed letter is a first attempt again, and must not carry the old verdict: a
// letter that fails again is archived with its new reason, not with two.
func TestAReplayedLetterIsAFirstAttemptWithoutItsOldVerdict(t *testing.T) {
	letter := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "event_id", Value: []byte("7")},
		{Key: "retry_attempt", Value: []byte("3")},
		{Key: "retry_origin_topic", Value: []byte("payments.main")},
		{Key: "dlq_class", Value: []byte("exhausted")},
		{Key: "dlq_reason", Value: []byte("lock timeout")},
		{Key: "dlq_origin_topic", Value: []byte("retry.10m")},
	}}

	want := map[string]string{
		"event_id":           "7",
		"retry_attempt":      "1",
		"retry_origin_topic": "payments.main",
	}
	if diff := cmp.Diff(want, headerMap(replayHeaders(letter))); diff != "" {
		t.Errorf("replayed headers (-want +got):\n%s", diff)
	}
}

// A reason is whatever an error chain says, and a driver can say a lot. A letter that
// outgrows the dead letter topic's limit cannot be archived, and the partition stops.
func TestADeadLetterReasonIsBoundedAndStillReadable(t *testing.T) {
	type args struct {
		reason error
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr error
	}{
		{name: "short reason: kept whole", args: args{reason: errors.New("lock timeout")}, want: "lock timeout"},
		{name: "long reason: cut at the limit", args: args{reason: errors.New(strings.Repeat("x", 5000))}, want: strings.Repeat("x", reasonLimit)},
		{name: "cut through a multibyte rune: no broken character left", args: args{reason: errors.New(strings.Repeat("x", reasonLimit-1) + "ї")}, want: strings.Repeat("x", reasonLimit-1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, boundedReason(tt.args.reason)); diff != "" {
				t.Errorf("reason (-want +got):\n%s", diff)
			}
		})
	}
}

func headerMap(headers []kgo.RecordHeader) map[string]string {
	byKey := make(map[string]string, len(headers))
	for _, h := range headers {
		byKey[h.Key] = string(h.Value)
	}
	return byKey
}
