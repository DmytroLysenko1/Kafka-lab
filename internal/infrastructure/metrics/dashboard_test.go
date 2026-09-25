package metrics_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
)

const dashboardPath = "../../../deploy/grafana/dashboards/payments.json"

// exported are the metric names that come from the cluster rather than from this process:
// kafka-exporter publishes them, and Prometheus itself publishes up. They are listed rather
// than gathered because nothing in this test can start either.
var exported = []string{
	"up",
	"kafka_brokers",
	"kafka_consumergroup_lag",
	"kafka_consumergroup_members",
	"kafka_consumergroup_current_offset",
	"kafka_topic_partition_under_replicated_partition",
}

// A panel whose query names a metric nothing publishes is a panel that is blank when it is
// needed, and nothing fails when the name drifts: the dashboard is JSON, the metric is Go,
// and no build sees both. This test is the only thing that does.
func TestEveryDashboardQueryNamesAMetricSomethingPublishes(t *testing.T) {
	known := publishedNames(t)
	known = append(known, exported...)

	for _, panel := range dashboardPanels(t) {
		for _, query := range panel.queries() {
			for _, name := range metricNames(query) {
				if !slices.Contains(known, name) {
					t.Errorf("panel %q queries %q, which nothing publishes\n  query: %s",
						panel.Title, name, query)
				}
			}
		}
	}
}

// The reverse direction, as a warning rather than a failure: a metric this service goes to
// the trouble of collecting and no panel reads is work done for nobody. It is not an error
// — a metric may exist for an alert or an ad-hoc query — but it is worth seeing.
func TestMetricsThisServiceCollectsAreReadBySomePanel(t *testing.T) {
	var queries string
	for _, panel := range dashboardPanels(t) {
		queries += strings.Join(panel.queries(), "\n")
	}

	for _, name := range publishedNames(t) {
		if !ours(name) || strings.Contains(queries, name) {
			continue
		}
		t.Logf("no panel reads %s", name)
	}
}

// ours separates what this service defines from the Go runtime and process collectors it
// also registers: nobody dashboards go_gc_duration_seconds here, and listing it would bury
// the one metric that is genuinely collected and never read.
func ours(name string) bool {
	for _, prefix := range []string{"outbox_", "payment_", "http_"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func publishedNames(t *testing.T) []string {
	t.Helper()

	observed := metrics.New()
	observed.Swept(1, time.Millisecond, nil)
	observed.Backlog(1)
	observed.Handled(true, time.Millisecond)
	observed.DeadLettered("undecodable")
	observed.Retried("payments-consumer.retry.5s")
	observed.Failed("handle")
	observed.Served("POST /payments", "201", time.Millisecond)

	families, err := observed.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gathering: %v", err)
	}

	var names []string
	for _, family := range families {
		name := family.GetName()
		names = append(names, name)
		// Only a histogram is queried through series Prometheus derives from it, which is
		// what a panel's histogram_quantile actually names. Inventing those suffixes for a
		// counter would let a typo through and would bury the reverse check in noise.
		if family.GetType().String() == "HISTOGRAM" {
			names = append(names, name+"_bucket", name+"_sum", name+"_count")
		}
	}
	return names
}

type panel struct {
	Title   string `json:"title"`
	Targets []struct {
		Expr string `json:"expr"`
	} `json:"targets"`
}

func (p panel) queries() []string {
	var out []string
	for _, target := range p.Targets {
		if target.Expr != "" {
			out = append(out, target.Expr)
		}
	}
	return out
}

func dashboardPanels(t *testing.T) []panel {
	t.Helper()

	raw, err := os.ReadFile(filepath.Clean(dashboardPath))
	if err != nil {
		t.Fatalf("reading the dashboard: %v", err)
	}
	var dashboard struct {
		Panels []panel `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dashboard); err != nil {
		t.Fatalf("parsing the dashboard: %v", err)
	}
	if len(dashboard.Panels) == 0 {
		t.Fatal("the dashboard has no panels, so this test would pass by having nothing to check")
	}
	return dashboard.Panels
}

// metricNames returns the identifiers in a query that name a metric. Everything else that
// is spelled like an identifier in PromQL is stripped first: label sets, the label lists of
// by/without/on/ignoring, and the functions and aggregators, which are the identifiers a
// "(" follows.
func metricNames(query string) []string {
	stripped := labelSet.ReplaceAllString(query, "")
	stripped = grouping.ReplaceAllString(stripped, " ")

	var names []string
	for _, match := range identifier.FindAllString(stripped, -1) {
		name := strings.TrimSpace(match)
		if strings.HasSuffix(name, "(") || slices.Contains(promQLKeywords, name) {
			continue
		}
		names = append(names, name)
	}
	return names
}

var (
	labelSet   = regexp.MustCompile(`\{[^}]*\}`)
	grouping   = regexp.MustCompile(`\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)`)
	identifier = regexp.MustCompile(`\b[a-zA-Z_][a-zA-Z0-9_]*\b\s*\(?`)

	promQLKeywords = []string{"by", "without", "on", "ignoring", "and", "or", "unless", "bool", "offset", "group_left", "group_right"}
)
