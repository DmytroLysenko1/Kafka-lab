// Package metrics is where the names live. A metric name, its labels and its unit are a
// wire format like any other: the services state what happened in their own words, and
// this package decides what Prometheus will call it.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry keeps the collectors of one process. It is handed to the admin server that
// serves them and to the adapters that write to them; nothing else needs to know.
type Registry struct {
	registry *prometheus.Registry

	outboxPublished  prometheus.Counter
	outboxSweeps     *prometheus.CounterVec
	outboxSweepTime  prometheus.Histogram
	outboxBacklog    prometheus.Gauge
	eventsHandled    *prometheus.CounterVec
	eventsFailed     *prometheus.CounterVec
	eventsDeadLetter *prometheus.CounterVec
	eventsRetried    *prometheus.CounterVec
	handleTime       prometheus.Histogram
	requests         *prometheus.CounterVec
	requestTime      *prometheus.HistogramVec
}

func New() *Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	metrics := &Registry{
		registry: registry,
		outboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_records_published_total",
			Help: "Outbox records the relay published and marked, after the broker acknowledged them.",
		}),
		outboxSweeps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_sweeps_total",
			Help: "Outbox sweeps, by outcome.",
		}, []string{"outcome"}),
		outboxSweepTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "outbox_sweep_duration_seconds",
			Help:    "How long one outbox sweep took, claim to commit.",
			Buckets: prometheus.DefBuckets,
		}),
		outboxBacklog: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_backlog_records",
			Help: "Outbox records waiting to be published, as of the last sweep.",
		}),
		eventsHandled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "payment_events_handled_total",
			Help: "Payment events the consumer handled, by what it did with them.",
		}, []string{"outcome"}),
		eventsDeadLetter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "payment_events_dead_lettered_total",
			Help: "Payment events archived to the dead letter topic, by why.",
		}, []string{"reason"}),
		eventsRetried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "payment_events_retried_total",
			Help: "Payment events moved aside into a retry tier, by the tier they went to.",
		}, []string{"tier"}),
		eventsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "payment_events_failed_total",
			Help: "Failures on the consumer's path, by the stage that failed.",
		}, []string{"stage"}),
		handleTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "payment_event_handling_duration_seconds",
			Help:    "How long one event took from decode to committed database write.",
			Buckets: prometheus.DefBuckets,
		}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests, by route and status.",
		}, []string{"route", "status"}),
		requestTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "How long a request took, by route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route"}),
	}

	registry.MustRegister(
		metrics.outboxPublished,
		metrics.outboxSweeps,
		metrics.outboxSweepTime,
		metrics.outboxBacklog,
		metrics.eventsHandled,
		metrics.eventsFailed,
		metrics.eventsDeadLetter,
		metrics.eventsRetried,
		metrics.handleTime,
		metrics.requests,
		metrics.requestTime,
	)
	return metrics
}

func (m *Registry) Gatherer() prometheus.Gatherer { return m.registry }

// Swept records one pass of the relay.
func (m *Registry) Swept(published int, took time.Duration, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "failed"
	}
	m.outboxSweeps.WithLabelValues(outcome).Inc()
	m.outboxSweepTime.Observe(took.Seconds())
	m.outboxPublished.Add(float64(published))
}

// Backlog is what an operator watches during an outage: it grows while the broker is gone
// and falls back once the relay drains. It is set only when it was actually read, so a
// database that cannot answer leaves the last known value standing instead of a zero.
func (m *Registry) Backlog(records int) {
	m.outboxBacklog.Set(float64(records))
}

// Handled records what the consumer did with one event. "duplicate" is not a failure: it
// is at-least-once delivery working, and an operator who cannot see the difference between
// a duplicate and a fresh event cannot tell a healthy replay from a broken one.
func (m *Registry) Handled(counted bool, took time.Duration) {
	outcome := "duplicate"
	if counted {
		outcome = "counted"
	}
	m.eventsHandled.WithLabelValues(outcome).Inc()
	m.handleTime.Observe(took.Seconds())
}

// DeadLettered takes a class that is already a bounded set — undecodable, refused,
// exhausted — never the error text, which carries identifiers and would turn a label into
// unbounded cardinality.
func (m *Registry) DeadLettered(class string) {
	m.eventsDeadLetter.WithLabelValues(class).Inc()
}

// Retried takes the tier a record moved into, a set as long as the chain. A rising rate
// on the first tier alone is contention that clears; the same rate on the last tier is
// contention that does not, and the dead letter panel is about to follow it.
func (m *Registry) Retried(tier string) {
	m.eventsRetried.WithLabelValues(tier).Inc()
}

// Failed takes the stage that failed, which is a fixed four-value set. Without it every
// failure on the consumer's path is invisible: the handled counter simply stops moving,
// which is what no traffic looks like too. A crash-looping consumer and an idle one are
// the same picture until this exists.
func (m *Registry) Failed(stage string) {
	m.eventsFailed.WithLabelValues(stage).Inc()
}

func (m *Registry) Served(route, status string, took time.Duration) {
	m.requests.WithLabelValues(route, status).Inc()
	m.requestTime.WithLabelValues(route).Observe(took.Seconds())
}

// Discard hears everything and keeps nothing, like io.Discard: for a one-shot tool that
// has no scrape to publish to, and for an experiment or a test that counts what reached
// the database and the topics rather than what a dashboard would have shown.
type Discard struct{}

func (Discard) Swept(int, time.Duration, error)      {}
func (Discard) Backlog(int)                          {}
func (Discard) Handled(bool, time.Duration)          {}
func (Discard) Retried(string)                       {}
func (Discard) DeadLettered(string)                  {}
func (Discard) Failed(string)                        {}
func (Discard) Served(string, string, time.Duration) {}
