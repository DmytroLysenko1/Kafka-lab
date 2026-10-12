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
	outboxSweepTime  *prometheus.HistogramVec
	outboxBacklog    prometheus.Gauge
	eventsHandled    *prometheus.CounterVec
	eventsFailed     *prometheus.CounterVec
	eventsDeadLetter *prometheus.CounterVec
	eventsRetried    *prometheus.CounterVec
	handleTime       *prometheus.HistogramVec
	requests         *prometheus.CounterVec
	requestTime      *prometheus.HistogramVec
}

// Each histogram's buckets follow what its operation actually takes. The bottom is shared:
// the drills measured requests at 1–3 ms p50 and 2–7 ms p95 at the client, through the
// ingress, so the work itself sits mostly under the default first bucket (5 ms), where a
// quantile panel could only interpolate — hence buckets down to half a millisecond. The top of
// each is the timeout that bounds the operation, so an operation cut off by it still lands in
// a bucket of its own instead of vanishing into +Inf, and nothing is kept above what can be
// observed. The buckets between are empty on a healthy day on purpose: they are what an
// incident fills.
func fastBuckets() []float64 {
	return []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}
}

// requestBuckets end at 11 s: the request's own deadline (5 s, interfaces/http) plus the commit
// that may finish past it on its own 5 s bound (postgres/storage.go), and a second of margin.
// The timer also covers reading the body, which the server's readTimeout bounds, not the
// deadline: a caller trickling a 150-byte body for seconds lands above 11 s, in +Inf — a slow
// client, which the top bucket is meant to keep apart from a slow service.
func requestBuckets() []float64 {
	return append(fastBuckets(), 2.5, 5, 11)
}

// sweepBuckets end at 36 s: the relay's 30 s sweepTimeout (cmd/outbox-relay) plus the rollback that may follow it
// on its own 5 s bound, and a second of margin. A sweep that waited out an unreachable broker
// takes the producer's 10 s deliveryTimeout plus its claim and rollback, so it lands in the
// 10–15 s bucket rather than at a ceiling indistinguishable from a slow success.
func sweepBuckets() []float64 {
	return append(fastBuckets(), 2.5, 5, 10, 15, 36)
}

// handlingBuckets end at 21 s: handlingTimeout (15 s) plus the commit or rollback that finishes
// on its own 5 s bound, and a second of margin — the timer wraps both. A contended record, which
// gives up after a 2 s lock wait and rolls back, lands in the 2–2.5 s bucket; its outcome label
// is what sets it apart, not a bound.
func handlingBuckets() []float64 {
	return append(fastBuckets(), 2.5, 5, 15, 21)
}

// New builds every service's metrics in one registry: each service fills only its own, and
// a series nobody writes to is simply never exported.
func New() *Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	metrics := &Registry{registry: registry}
	metrics.addRelay()
	metrics.addConsumer()
	metrics.addHTTP()
	return metrics
}

func (m *Registry) addRelay() {
	m.outboxPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_records_published_total",
		Help: "Outbox records the relay published and marked, after the broker acknowledged them.",
	})
	m.outboxSweeps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_sweeps_total",
		Help: "Outbox sweeps, by outcome.",
	}, []string{"outcome"})
	m.outboxSweepTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "outbox_sweep_duration_seconds",
		Help:    "How long one outbox sweep took, claim to commit, by outcome.",
		Buckets: sweepBuckets(),
	}, []string{"outcome"})
	m.outboxBacklog = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_backlog_records",
		Help: "Outbox records waiting to be published, counted by the relay on a tick of its own.",
	})
	m.registry.MustRegister(m.outboxPublished, m.outboxSweeps, m.outboxSweepTime, m.outboxBacklog)
}

func (m *Registry) addConsumer() {
	m.eventsHandled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "payment_events_handled_total",
		Help: "Payment events the consumer handled, by what it did with them.",
	}, []string{"outcome"})
	m.eventsDeadLetter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "payment_events_dead_lettered_total",
		Help: "Payment events archived to the dead letter topic, by why.",
	}, []string{"reason"})
	m.eventsRetried = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "payment_events_retried_total",
		Help: "Payment events moved aside into a retry tier, by the tier topic they went to.",
	}, []string{"topic"})
	m.eventsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "payment_events_failed_total",
		Help: "Failures on the consumer's path, by the stage that failed.",
	}, []string{"stage"})
	m.handleTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "payment_event_handling_duration_seconds",
		Help:    "How long one attempt at an event's database work took, by what it came to.",
		Buckets: handlingBuckets(),
	}, []string{"outcome"})
	m.registry.MustRegister(m.eventsHandled, m.eventsFailed, m.eventsDeadLetter, m.eventsRetried, m.handleTime)
}

func (m *Registry) addHTTP() {
	m.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests, by route and status.",
	}, []string{"route", "status"})
	m.requestTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "How long a request took, by route.",
		Buckets: requestBuckets(),
	}, []string{"route"})
	m.registry.MustRegister(m.requests, m.requestTime)
}

func (m *Registry) Gatherer() prometheus.Gatherer {
	return m.registry
}

// Swept records one pass of the relay. An empty pass — the relay finding nothing to publish,
// once a second when idle — is labelled apart, so the duration of real work is not averaged
// with the cost of looking.
func (m *Registry) Swept(published int, took time.Duration, err error) {
	outcome := sweepOutcome(published, err)
	m.outboxSweeps.WithLabelValues(outcome).Inc()
	m.outboxSweepTime.WithLabelValues(outcome).Observe(took.Seconds())
	m.outboxPublished.Add(float64(published))
}

const (
	sweepFailed    = "failed"
	sweepEmpty     = "empty"
	sweepPublished = "published"
)

func sweepOutcome(published int, err error) string {
	switch {
	case err != nil:
		return sweepFailed
	case published == 0:
		return sweepEmpty
	}
	return sweepPublished
}

// Backlog is what an operator watches during an outage: it grows while the broker is gone
// and falls back once the relay drains. It is set only when it was actually read, so a
// database that cannot answer leaves the last known value standing instead of a zero.
func (m *Registry) Backlog(records int) {
	m.outboxBacklog.Set(float64(records))
}

// Handled records what the consumer did with one event, in the consumer's own words: counted,
// or duplicate. "duplicate" is not a failure: it is at-least-once delivery working, and an
// operator who cannot see the difference between a duplicate and a fresh event cannot tell a
// healthy replay from a broken one.
func (m *Registry) Handled(outcome string) {
	m.eventsHandled.WithLabelValues(outcome).Inc()
}

// HandlingTook takes an outcome from the consumer's fixed set — counted, duplicate,
// contended, refused, failed — so the time a record spent waiting on a lock or failing is on
// the same histogram as the successes, apart from them.
func (m *Registry) HandlingTook(outcome string, took time.Duration) {
	m.handleTime.WithLabelValues(outcome).Observe(took.Seconds())
}

// DeadLettered takes a class that is already a bounded set — undecodable, refused,
// exhausted — never the error text, which carries identifiers and would turn a label into
// unbounded cardinality.
func (m *Registry) DeadLettered(class string) {
	m.eventsDeadLetter.WithLabelValues(class).Inc()
}

// Retried takes the tier topic a record moved into, a set as long as the chain. A rising rate
// on the first tier alone is contention that clears; the same rate on the last tier is
// contention that does not, and the dead letter panel is about to follow it.
func (m *Registry) Retried(topic string) {
	m.eventsRetried.WithLabelValues(topic).Inc()
}

// Failed takes the stage that failed, which is a fixed five-value set. Without it every
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
func (Discard) Handled(string)                       {}
func (Discard) HandlingTook(string, time.Duration)   {}
func (Discard) Retried(string)                       {}
func (Discard) DeadLettered(string)                  {}
func (Discard) Failed(string)                        {}
func (Discard) Served(string, string, time.Duration) {}
