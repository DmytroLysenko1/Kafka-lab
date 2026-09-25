package metrics_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
)

var errBroker = errors.New("the broker is not answering")

// The names and labels are the contract an operator's dashboard and alerts are written
// against: renaming one of them silently is how a panel goes blank at three in the morning.
func TestTheRelayReportsPublishedRecordsSweepOutcomesAndItsBacklog(t *testing.T) {
	observed := metrics.New()

	observed.Swept(3, 20*time.Millisecond, nil)
	observed.Swept(0, 5*time.Millisecond, errBroker)
	observed.Backlog(7)

	expected := `
# HELP outbox_backlog_records Outbox records waiting to be published, as of the last sweep.
# TYPE outbox_backlog_records gauge
outbox_backlog_records 7
# HELP outbox_records_published_total Outbox records the relay published and marked, after the broker acknowledged them.
# TYPE outbox_records_published_total counter
outbox_records_published_total 3
# HELP outbox_sweeps_total Outbox sweeps, by outcome.
# TYPE outbox_sweeps_total counter
outbox_sweeps_total{outcome="failed"} 1
outbox_sweeps_total{outcome="ok"} 1
`
	if err := testutil.GatherAndCompare(observed.Gatherer(), strings.NewReader(expected),
		"outbox_backlog_records", "outbox_records_published_total", "outbox_sweeps_total"); err != nil {
		t.Error(err)
	}
}

// A duplicate is at-least-once delivery working, not a failure — an operator who cannot
// tell one from a fresh event cannot tell a healthy replay from a broken consumer.
func TestTheConsumerSeparatesCountedEventsFromDuplicates(t *testing.T) {
	observed := metrics.New()

	observed.Handled(true, time.Millisecond)
	observed.Handled(false, time.Millisecond)
	observed.Handled(false, time.Millisecond)
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
	observed.Handled(true, 250*time.Millisecond)
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
