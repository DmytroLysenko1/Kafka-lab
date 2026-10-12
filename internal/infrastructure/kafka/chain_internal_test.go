package kafka

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"
)

// The chain the consumer runs and the tiers the replayer may write into are the same list: a
// tier added to one and not the other would either never be read or never be replayed into.
func TestThePaymentsChainIsTheMainTopicFollowedByEveryTier(t *testing.T) {
	want := []Stage{
		{Topic: "payments.main", Group: "payments-consumer", Tier: 0, Next: "payments-consumer.retry.5s"},
		{Topic: "payments-consumer.retry.5s", Group: "payments-consumer.retry.5s", Tier: 1, Delay: 5 * time.Second, Next: "payments-consumer.retry.1m"},
		{Topic: "payments-consumer.retry.1m", Group: "payments-consumer.retry.1m", Tier: 2, Delay: time.Minute, Next: "payments-consumer.retry.10m"},
		{Topic: "payments-consumer.retry.10m", Group: "payments-consumer.retry.10m", Tier: 3, Delay: 10 * time.Minute},
	}
	if diff := cmp.Diff(want, PaymentsChain("payments.main", "payments-consumer")); diff != "" {
		t.Errorf("chain mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff("payments-consumer.retry.5s", FirstRetryTier()); diff != "" {
		t.Errorf("first tier mismatch (-want +got):\n%s", diff)
	}
}

func TestOnlyATierOfThisGroupIsATierToReplayInto(t *testing.T) {
	tests := []struct {
		name string
		args string
		want bool
	}{
		{name: "the first tier", args: "payments-consumer.retry.5s", want: true},
		{name: "the last tier", args: "payments-consumer.retry.10m", want: true},
		{name: "the main topic, which every group reads", args: "payments.main", want: false},
		{name: "a topic that only looks like a tier", args: "payments-consumer.retry.forever", want: false},
		{name: "the dead letter topic", args: "payments-consumer.dlq", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, IsRetryTier(tt.args)); diff != "" {
				t.Errorf("IsRetryTier(%q) (-want +got):\n%s", tt.args, diff)
			}
		})
	}
}

// The tiers are declared twice — here, and as topics in the catalog topicctl applies — and
// nothing else compares the two. A tier the code names and the catalog lacks is a topic that
// never exists; a delay that disagrees with the name would make a record wait for something
// other than what every topic list says.
func TestEveryTierIsATopicInTheCatalogWithTheDelayItsNameSays(t *testing.T) {
	const catalog = "../../../deploy/topics"
	type tier struct {
		Topic       string
		Delay       time.Duration
		NamedDelay  time.Duration
		InCatalog   bool
		CatalogName string
	}
	var got, want []tier
	for _, stage := range retryTiers() {
		named, err := time.ParseDuration(strings.TrimPrefix(stage.Topic, "payments-consumer.retry."))
		if err != nil {
			t.Fatalf("tier %s: no delay in its name: %v", stage.Topic, err)
		}
		declared, readErr := os.ReadFile(filepath.Join(catalog, stage.Topic+".yaml"))
		got = append(got, tier{
			Topic:       stage.Topic,
			Delay:       stage.Delay,
			NamedDelay:  named,
			InCatalog:   readErr == nil,
			CatalogName: catalogName(declared),
		})
		want = append(want, tier{Topic: stage.Topic, Delay: stage.Delay, NamedDelay: stage.Delay, InCatalog: true, CatalogName: stage.Topic})
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("tiers against the catalog (-want +got):\n%s", diff)
	}

	cataloged, err := filepath.Glob(filepath.Join(catalog, "payments-consumer.retry.*.yaml"))
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	declared := lo.Map(retryTiers(), func(stage Stage, _ int) string { return stage.Topic + ".yaml" })
	listed := lo.Map(cataloged, func(path string, _ int) string { return filepath.Base(path) })
	if diff := cmp.Diff(declared, listed, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("retry topics in the catalog the chain does not run, or the reverse (-chain +catalog):\n%s", diff)
	}
}

// catalogName is the topic's meta.name in a topicctl file: the first "name:" line.
func catalogName(file []byte) string {
	for line := range strings.Lines(string(file)) {
		if name, ok := strings.CutPrefix(strings.TrimSpace(line), "name:"); ok {
			return strings.TrimSpace(name)
		}
	}
	return ""
}
