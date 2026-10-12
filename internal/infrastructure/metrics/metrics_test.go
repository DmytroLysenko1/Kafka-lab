package metrics_test

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
)

var errBroker = errors.New("the broker is not answering")

// The names and labels are the contract an operator's dashboard and alerts are written
// against: renaming one of them silently is how a panel goes blank at three in the morning.
func TestTheRelayReportsPublishedRecordsSweepOutcomesAndItsBacklog(t *testing.T) {
	observed := metrics.New()

	observed.Swept(3, 20*time.Millisecond, nil)
	observed.Swept(0, time.Millisecond, nil)
	observed.Swept(0, 5*time.Millisecond, errBroker)
	observed.Backlog(7)

	expected := `
# HELP outbox_backlog_records Outbox records waiting to be published, counted by the relay on a tick of its own.
# TYPE outbox_backlog_records gauge
outbox_backlog_records 7
# HELP outbox_records_published_total Outbox records the relay published and marked, after the broker acknowledged them.
# TYPE outbox_records_published_total counter
outbox_records_published_total 3
# HELP outbox_sweeps_total Outbox sweeps, by outcome.
# TYPE outbox_sweeps_total counter
outbox_sweeps_total{outcome="empty"} 1
outbox_sweeps_total{outcome="failed"} 1
outbox_sweeps_total{outcome="published"} 1
`
	if err := testutil.GatherAndCompare(observed.Gatherer(), strings.NewReader(expected),
		"outbox_backlog_records", "outbox_records_published_total", "outbox_sweeps_total"); err != nil {
		t.Error(err)
	}
}

// The relay reports sweeps and the backlog from two goroutines — the backlog on a clock of
// its own, so that it keeps moving while a sweep is stuck on a dead cluster. This is the
// contract that makes that safe, proved against the real type under -race rather than
// assumed from the library underneath it.
func TestSweepsAndTheBacklogCanBeReportedAtTheSameTime(t *testing.T) {
	observed := metrics.New()
	const reports = 1000

	var reporters sync.WaitGroup
	reporters.Go(func() {
		for range reports {
			observed.Swept(1, time.Millisecond, nil)
		}
	})
	reporters.Go(func() {
		for backlog := range reports {
			observed.Backlog(backlog)
		}
	})
	reporters.Wait()

	expected := `
# HELP outbox_backlog_records Outbox records waiting to be published, counted by the relay on a tick of its own.
# TYPE outbox_backlog_records gauge
outbox_backlog_records 999
# HELP outbox_records_published_total Outbox records the relay published and marked, after the broker acknowledged them.
# TYPE outbox_records_published_total counter
outbox_records_published_total 1000
`
	if err := testutil.GatherAndCompare(observed.Gatherer(), strings.NewReader(expected),
		"outbox_backlog_records", "outbox_records_published_total"); err != nil {
		t.Error(err)
	}
}

// A duplicate is at-least-once delivery working, not a failure — an operator who cannot
// tell one from a fresh event cannot tell a healthy replay from a broken consumer.
func TestTheConsumerSeparatesCountedEventsFromDuplicates(t *testing.T) {
	observed := metrics.New()

	observed.Handled("counted")
	observed.Handled("duplicate")
	observed.Handled("duplicate")
	observed.DeadLettered("undecodable")

	expected := `
# HELP payment_events_dead_lettered_total Payment events archived to the dead letter topic, by why.
# TYPE payment_events_dead_lettered_total counter
payment_events_dead_lettered_total{reason="undecodable"} 1
# HELP payment_events_handled_total Payment events the consumer handled, by what it did with them.
# TYPE payment_events_handled_total counter
payment_events_handled_total{outcome="counted"} 1
payment_events_handled_total{outcome="duplicate"} 2
`
	if err := testutil.GatherAndCompare(observed.Gatherer(), strings.NewReader(expected),
		"payment_events_handled_total", "payment_events_dead_lettered_total"); err != nil {
		t.Error(err)
	}
}

// A consumer that fails every record draws the handled counter to zero — which is exactly
// what a consumer with no traffic draws. exp-11 crash-looped 124–125 times in 30 s and left
// no mark anywhere; this is the counter that tells the two apart, and the stage is what
// says whether to look at the broker, the handler, the commit or the dead letter topic.
func TestEveryFailureOnTheConsumersPathIsCountedByStage(t *testing.T) {
	observed := metrics.New()

	observed.Failed("fetch")
	observed.Failed("handle")
	observed.Failed("handle")
	observed.Failed("commit")
	observed.Failed("dead_letter")

	expected := `
# HELP payment_events_failed_total Failures on the consumer's path, by the stage that failed.
# TYPE payment_events_failed_total counter
payment_events_failed_total{stage="commit"} 1
payment_events_failed_total{stage="dead_letter"} 1
payment_events_failed_total{stage="fetch"} 1
payment_events_failed_total{stage="handle"} 2
`
	if err := testutil.GatherAndCompare(observed.Gatherer(), strings.NewReader(expected),
		"payment_events_failed_total"); err != nil {
		t.Error(err)
	}
}

// A tier is a place, not an error: traffic that stops at the first tier is contention that
// cleared, and traffic reaching the last is contention that did not. Summed into one
// number, the two look the same until the exhausted dead letters arrive.
func TestRecordsMovedAsideAreCountedByTheTierTheyWentTo(t *testing.T) {
	observed := metrics.New()

	observed.Retried("payments-consumer.retry.5s")
	observed.Retried("payments-consumer.retry.5s")
	observed.Retried("payments-consumer.retry.1m")
	observed.DeadLettered("exhausted")

	expected := `
# HELP payment_events_dead_lettered_total Payment events archived to the dead letter topic, by why.
# TYPE payment_events_dead_lettered_total counter
payment_events_dead_lettered_total{reason="exhausted"} 1
# HELP payment_events_retried_total Payment events moved aside into a retry tier, by the tier topic they went to.
# TYPE payment_events_retried_total counter
payment_events_retried_total{topic="payments-consumer.retry.1m"} 1
payment_events_retried_total{topic="payments-consumer.retry.5s"} 2
`
	if err := testutil.GatherAndCompare(observed.Gatherer(), strings.NewReader(expected),
		"payment_events_retried_total", "payment_events_dead_lettered_total"); err != nil {
		t.Error(err)
	}
}

func TestRequestsAreCountedByRouteAndStatus(t *testing.T) {
	observed := metrics.New()

	observed.Served("POST /payments", "201", 3*time.Millisecond)
	observed.Served("POST /payments", "409", time.Millisecond)

	expected := `
# HELP http_requests_total HTTP requests, by route and status.
# TYPE http_requests_total counter
http_requests_total{route="POST /payments",status="201"} 1
http_requests_total{route="POST /payments",status="409"} 1
`
	if err := testutil.GatherAndCompare(observed.Gatherer(), strings.NewReader(expected), "http_requests_total"); err != nil {
		t.Error(err)
	}
}

// A timing is a histogram rather than an average this process computed and forgot the
// shape of: what a dashboard divides is the count and the sum.
func TestTimingsAreObservedAsHistograms(t *testing.T) {
	observed := metrics.New()

	observed.Swept(1, 100*time.Millisecond, nil)
	observed.HandlingTook("counted", 250*time.Millisecond)
	observed.Served("GET /merchants/{id}/total", "200", 10*time.Millisecond)

	for _, name := range []string{
		"outbox_sweep_duration_seconds",
		"payment_event_handling_duration_seconds",
		"http_request_duration_seconds",
	} {
		count, err := testutil.GatherAndCount(observed.Gatherer(), name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if count != 1 {
			t.Errorf("%s: collected %d series, want 1", name, count)
		}
	}
}

// The top bucket of each histogram is the timeout that bounds its operation, so the slowest
// thing that can happen still lands in a bucket of its own: a sweep that waited out an
// unreachable broker is told apart from one that took ten seconds of real work, and a record
// that waited out the lock timeout is told apart from a fast one — instead of both vanishing
// into +Inf, where a quantile panel draws a flat line at the last bound.
func TestTheSlowestOutcomeOfEachOperationLandsInsideItsTopBucket(t *testing.T) {
	type args struct {
		observe func(*metrics.Registry)
		metric  string
		outcome string
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "a sweep that waited out the producer's 10 s delivery timeout",
			args: args{
				observe: func(observed *metrics.Registry) { observed.Swept(0, 11*time.Second, errBroker) },
				metric:  "outbox_sweep_duration_seconds",
				outcome: "failed",
			},
			want: want{LowestBound: 15, BelowIt: 10},
		},
		{
			name: "a sweep cut off by the relay's 30 s sweep timeout, then rolled back",
			args: args{
				observe: func(observed *metrics.Registry) { observed.Swept(0, 35*time.Second, errBroker) },
				metric:  "outbox_sweep_duration_seconds",
				outcome: "failed",
			},
			want: want{LowestBound: 36, BelowIt: 15},
		},
		{
			name: "a record that gave up on a lock after 2 s",
			args: args{
				observe: func(observed *metrics.Registry) { observed.HandlingTook("contended", 2010*time.Millisecond) },
				metric:  "payment_event_handling_duration_seconds",
				outcome: "contended",
			},
			want: want{LowestBound: 2.5, BelowIt: 1},
		},
		{
			name: "a record cut off by the 15 s handling timeout, then rolled back",
			args: args{
				observe: func(observed *metrics.Registry) { observed.HandlingTook("failed", 20*time.Second) },
				metric:  "payment_event_handling_duration_seconds",
				outcome: "failed",
			},
			want: want{LowestBound: 21, BelowIt: 15},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observed := metrics.New()
			tt.args.observe(observed)

			got := lowestBucketHolding(t, observed, tt.args.metric, tt.args.outcome)

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("buckets (-want +got):\n%s", diff)
			}
		})
	}
}

type want struct {
	LowestBound float64
	BelowIt     float64
}

// lowestBucketHolding finds the smallest bound whose cumulative count includes the one
// observation, and the bound just below it. +Inf is never reported as a bound.
func lowestBucketHolding(t *testing.T, observed *metrics.Registry, metric, outcome string) want {
	t.Helper()
	families, err := observed.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gathering: %v", err)
	}
	for _, family := range families {
		if family.GetName() != metric {
			continue
		}
		for _, series := range family.GetMetric() {
			if !hasLabel(series.GetLabel(), "outcome", outcome) {
				continue
			}
			previous := 0.0
			for _, bucket := range series.GetHistogram().GetBucket() {
				if bucket.GetCumulativeCount() > 0 {
					return want{LowestBound: bucket.GetUpperBound(), BelowIt: previous}
				}
				previous = bucket.GetUpperBound()
			}
		}
	}
	t.Fatalf("%s{outcome=%q}: the observation is in no finite bucket", metric, outcome)
	return want{}
}

func hasLabel(labels []*dto.LabelPair, name, value string) bool {
	return slices.ContainsFunc(labels, func(label *dto.LabelPair) bool {
		return label.GetName() == name && label.GetValue() == value
	})
}
