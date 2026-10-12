package kafka

import (
	"slices"
	"time"
)

// retryTiers are the payments consumer group's tiers, each a group of its own: a tier waiting
// ten minutes for its head record must not hold a rebalance of the main group, and a separate
// group is what keeps their offsets and members apart. The topics are the catalog's
// (deploy/topics/), and each carries its delay in its name so that nobody reading a topic list
// has to guess what it holds records for. Declared once: the consumer builds its chain from
// them and the replayer refuses to replay anywhere else.
func retryTiers() []Stage {
	return []Stage{
		{Topic: "payments-consumer.retry.5s",
			Group: "payments-consumer.retry.5s",
			Delay: 5 * time.Second,
		},
		{Topic: "payments-consumer.retry.1m",
			Group: "payments-consumer.retry.1m",
			Delay: time.Minute,
		},
		{Topic: "payments-consumer.retry.10m",
			Group: "payments-consumer.retry.10m",
			Delay: 10 * time.Minute,
		},
	}
}

// PaymentsChain is the main topic, read by group, followed by the retry tiers.
func PaymentsChain(topic, group string) []Stage {
	return Chain(append([]Stage{{
		Topic: topic,
		Group: group,
	}}, retryTiers()...)...)
}

// FirstRetryTier is where a replay sends letters back: the start of the chain, not the main
// topic, which every group reading payments reads too.
func FirstRetryTier() string {
	return retryTiers()[0].Topic
}

// IsRetryTier is whether topic is one of this group's tiers, by name and not by prefix: a
// topic that merely looks like a tier is no place to replay money events into.
func IsRetryTier(topic string) bool {
	return slices.ContainsFunc(retryTiers(), func(tier Stage) bool {
		return tier.Topic == topic
	})
}
