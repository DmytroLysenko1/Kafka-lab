package main

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// franz-go retries a retriable batch error without logging it above debug level. The only
// place it says what happened to each batch is the debug summary of every produce response
// ("produced", key "to"). A failed batch it will resend is marked retrying@<offset>,<n>(<err>)
// when it heads its partition and skipped@<offset>,<n>(<err>) when an earlier batch of the
// partition had already failed; both are counted by error. A batch that was written but whose
// success it ignores, because it rewound behind an earlier failure, is skipped@<offset>=><end>.
// A timed-out append and an ignored success are the two ways the plain producer writes a
// record twice, which is what this run has to tell apart.
const (
	msgProduced = "produced"
	keyProduced = "to"
)

// Request-level failures: no produce response came back at all.
var requestFailed = []string{
	"produce request failed with a retryable error, retrying without a metadata update",
	"produce request failed, triggering metadata update",
	"random error while producing, requeueing unattempted request",
}

var (
	retryingMark = regexp.MustCompile(`(?:retrying|skipped)@-?\d+,(\d+)\(([^)]*)\)`)
	skippedMark  = regexp.MustCompile(`skipped@(-?\d+)=>(-?\d+)`)
)

// failures counts what the producer did with each batch it did not simply finish. It is
// handed to the client as its logger and reads nothing but the messages above.
type failures struct {
	mu              sync.Mutex
	retriedBatches  map[string]int
	retriedRecords  map[string]int
	requestFailures map[string]int
	skippedRecords  int
}

func newFailures() *failures {
	return &failures{
		retriedBatches:  make(map[string]int),
		retriedRecords:  make(map[string]int),
		requestFailures: make(map[string]int),
	}
}

func (f *failures) Level() kgo.LogLevel {
	return kgo.LogLevelDebug
}

func (f *failures) Log(_ kgo.LogLevel, msg string, keyvals ...any) {
	switch {
	case msg == msgProduced:
		f.produced(fmt.Sprint(value(keyvals, keyProduced)))
	case slices.Contains(requestFailed, msg):
		f.add(f.requestFailures, code(value(keyvals, "err")), 1)
	}
}

func (f *failures) produced(summary string) {
	// The marks match only digits, so a number fails to parse only on overflow, and such
	// a mark is left out rather than counted as zero.
	for _, mark := range retryingMark.FindAllStringSubmatch(summary, -1) {
		records, err := strconv.Atoi(mark[1])
		if err != nil {
			continue
		}
		name := code(mark[2])
		f.add(f.retriedBatches, name, 1)
		f.add(f.retriedRecords, name, records)
	}
	for _, mark := range skippedMark.FindAllStringSubmatch(summary, -1) {
		from, errFrom := strconv.Atoi(mark[1])
		to, errTo := strconv.Atoi(mark[2])
		if errFrom != nil || errTo != nil {
			continue
		}
		f.mu.Lock()
		f.skippedRecords += to - from
		f.mu.Unlock()
	}
}

func (f *failures) add(counts map[string]int, name string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	counts[name] += n
}

func (f *failures) lines() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	lines := []string{fmt.Sprintf("written, success ignored, resent\t%d records — rewound behind a failed batch", f.skippedRecords)}
	for _, name := range slices.Sorted(maps.Keys(f.retriedBatches)) {
		lines = append(lines, fmt.Sprintf("retried, %s\t%d batches, %d records — %s",
			name, f.retriedBatches[name], f.retriedRecords[name], meaning(name)))
	}
	for _, name := range slices.Sorted(maps.Keys(f.requestFailures)) {
		lines = append(lines, fmt.Sprintf("request failed, %s\t%d — no response, so whether it landed is unknown", name, f.requestFailures[name]))
	}
	if len(f.retriedBatches) == 0 && len(f.requestFailures) == 0 {
		lines = append(lines, "retried batches\tnone")
	}
	return lines
}

func value(keyvals []any, key string) any {
	for pair := range slices.Chunk(keyvals, 2) {
		if len(pair) == 2 && pair[0] == key {
			return pair[1]
		}
	}
	return nil
}

// code names an error by its Kafka code where it has one. The debug summary carries the
// error as text, "REQUEST_TIMED_OUT: The request timed out.", so the code is the part
// before the colon.
func code(v any) string {
	if err, ok := v.(error); ok {
		if known, ok := errors.AsType[*kerr.Error](err); ok {
			return known.Message
		}
		return err.Error()
	}
	text := fmt.Sprint(v)
	name, _, _ := strings.Cut(text, ":")
	return name
}

func meaning(code string) string {
	switch code {
	case kerr.RequestTimedOut.Message, kerr.NotEnoughReplicasAfterAppend.Message:
		return "appended before the refusal: the retry duplicates it unless the producer is idempotent"
	case kerr.NotEnoughReplicas.Message:
		return "refused before the append"
	default:
		return "not classified"
	}
}
