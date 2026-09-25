package kafka

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Stage is one topic of the chain and the group that reads it. The main topic is tier 0,
// with no delay; the n-th retry tier is tier n, whose records wait Delay after they entered
// it. A record that is contended again moves on to Next; on the last stage, where Next is
// empty, it is dead-lettered as exhausted instead. The tier is what a record's attempt
// count is taken from: the stage knows where it sits in the chain, and a header only
// says what whoever produced the record wrote in it.
type Stage struct {
	Topic string
	Group string
	Tier  int
	Delay time.Duration
	Next  string
}

// Class is why a record was archived: a bounded set, because each is a metric label and a
// replay filter, and a class nobody archives under would be a replay that silently finds
// nothing.
type Class string

const (
	ClassUndecodable Class = "undecodable"
	ClassRefused     Class = "refused"
	ClassExhausted   Class = "exhausted"
)

var ErrUnknownClass = errors.New("kafka: unknown dead letter class")

// ParseClass accepts only a class something is archived under: a replay of any other name
// would read the whole dead letter topic and find nothing, and say so as if that were true.
func ParseClass(name string) (Class, error) {
	class := Class(name)
	if !slices.Contains([]Class{
		ClassUndecodable,
		ClassRefused,
		ClassExhausted,
	}, class) {
		return "", fmt.Errorf("%w: %q", ErrUnknownClass, name)
	}
	return class, nil
}

const (
	headerRetryAttempt              = "retry_attempt"
	headerRetryOriginTopic          = "retry_origin_topic"
	headerRetryOriginPartition      = "retry_origin_partition"
	headerRetryOriginOffset         = "retry_origin_offset"
	headerRetryFirstFailedAt        = "retry_first_failed_at"
	headerDeadLetterClass           = "dlq_class"
	headerDeadLetterReason          = "dlq_reason"
	headerDeadLetterOriginTopic     = "dlq_origin_topic"
	headerDeadLetterOriginPartition = "dlq_origin_partition"
	headerDeadLetterOriginOffset    = "dlq_origin_offset"
	headerDeadLetterArchivedAt      = "dlq_archived_at"
	headerDeadLetterPrefix          = "dlq_"
)

// Chain links stages in the order given into one retry chain: each stage's Next is the
// topic of the one after it, the last has none and dead-letters what it cannot handle, and
// each Tier is its position — the main topic is tier 0. Both are derived here so that no
// caller can link a tier to the wrong next topic or give two stages the same attempt count.
func Chain(stages ...Stage) []Stage {
	linked := slices.Clone(stages)
	for position := range linked {
		linked[position].Tier = position
		linked[position].Next = ""
		if position+1 < len(linked) {
			linked[position].Next = linked[position+1].Topic
		}
	}
	return linked
}

// untilDue is how long a record still has to wait before this stage may handle it. The
// retry topics stamp records with LogAppendTime, so the timestamp is the moment the record
// entered the stage by the broker's clock, not by the clock of whoever sent it there. A
// stage with no delay never waits: its timestamps are the producer's, and a producer clock
// running ahead of this one must not hold a record back.
func (s Stage) untilDue(record *kgo.Record, now time.Time) time.Duration {
	if s.Delay <= 0 {
		return 0
	}
	return max(record.Timestamp.Add(s.Delay).Sub(now), 0)
}

// clientOptions are the ones a stage needs because of what it is: a tier pauses partitions
// and resumes them on time, the main topic does neither.
func (s Stage) clientOptions() []kgo.Opt {
	if s.Delay <= 0 {
		return nil
	}
	return []kgo.Opt{kgo.FetchMaxWait(tierFetchWait)}
}

// retryHeaders is what a record carries into its next stage. Every header is replaced,
// never appended: a record that went through three tiers carries one count, not three. The
// origin is written on the first attempt, on the way out of the main topic, and kept after
// that, so a dead letter says where the payment was first read, and when it first failed,
// however far it travelled. On the first attempt it is overwritten, not kept: the only
// retry headers a record can carry on the main topic are ones its producer made up. The
// result is a new slice: the fetched record still belongs to the client that read it.
func retryHeaders(record *kgo.Record, attempt int, failedAt time.Time) []kgo.RecordHeader {
	headers := withHeader(record.Headers, headerRetryAttempt, strconv.Itoa(attempt))
	if attempt > 1 {
		return headers
	}
	headers = withHeader(headers, headerRetryOriginTopic, record.Topic)
	headers = withHeader(headers, headerRetryOriginPartition, strconv.Itoa(int(record.Partition)))
	headers = withHeader(headers, headerRetryOriginOffset, strconv.FormatInt(record.Offset, 10))
	return withHeader(headers, headerRetryFirstFailedAt, failedAt.UTC().Format(time.RFC3339))
}

// replayHeaders turns a dead letter back into a first attempt: the verdict of the dead
// letter topic and the old attempt count are dropped, and everything the payment carried
// — the event id above all, which is what deduplicates it — is kept.
func replayHeaders(record *kgo.Record) []kgo.RecordHeader {
	kept := slices.DeleteFunc(slices.Clone(record.Headers), func(h kgo.RecordHeader) bool {
		return h.Key == headerRetryAttempt || strings.HasPrefix(h.Key, headerDeadLetterPrefix)
	})
	return append(kept, kgo.RecordHeader{
		Key:   headerRetryAttempt,
		Value: []byte("1"),
	})
}

// reasonLimit keeps a dead letter under max.message.bytes whatever the error says: a letter
// too large for the dead letter topic cannot be archived, and the partition behind it
// stops exactly as it would without a dead letter topic at all.
const reasonLimit = 1024

func boundedReason(reason error) string {
	text := reason.Error()
	if len(text) <= reasonLimit {
		return text
	}
	return strings.ToValidUTF8(text[:reasonLimit], "")
}

func withHeader(headers []kgo.RecordHeader, key, value string) []kgo.RecordHeader {
	kept := slices.DeleteFunc(slices.Clone(headers), func(h kgo.RecordHeader) bool {
		return h.Key == key
	})
	return append(kept, kgo.RecordHeader{
		Key:   key,
		Value: []byte(value),
	})
}

// headerValue is empty for a header that is absent, which every caller treats the same as
// one that is present and empty: an event id nobody can deduplicate by, a class nobody
// replays.
func headerValue(headers []kgo.RecordHeader, key string) string {
	index := slices.IndexFunc(headers, func(h kgo.RecordHeader) bool {
		return h.Key == key
	})
	if index < 0 {
		return ""
	}
	return string(headers[index].Value)
}
