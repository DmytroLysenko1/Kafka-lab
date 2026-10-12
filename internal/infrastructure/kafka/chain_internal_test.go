package kafka

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
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
