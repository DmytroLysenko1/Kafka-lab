package kafka

import "github.com/samber/lo"

const (
	TopicPaymentsMain     = "payments.main"
	TopicPaymentsRetry5s  = "payments.retry.5s"
	TopicPaymentsRetry1m  = "payments.retry.1m"
	TopicPaymentsRetry10m = "payments.retry.10m"
	TopicPaymentsDLQ      = "payments.dlq"
)

const (
	labPartitions        int32 = 6
	labReplicationFactor int16 = 3

	configMinInSyncReplicas = "min.insync.replicas"
	minInSyncReplicas       = "2"
)

type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Config            map[string]string
}

func LabTopicNames() []string {
	return lo.Map(LabTopics(), func(spec TopicSpec, _ int) string {
		return spec.Name
	})
}

func LabTopics() []TopicSpec {
	return []TopicSpec{
		labTopic(TopicPaymentsMain),
		labTopic(TopicPaymentsRetry5s),
		labTopic(TopicPaymentsRetry1m),
		labTopic(TopicPaymentsRetry10m),
		labTopic(TopicPaymentsDLQ),
	}
}

func labTopic(name string) TopicSpec {
	return TopicSpec{
		Name:              name,
		Partitions:        labPartitions,
		ReplicationFactor: labReplicationFactor,
		Config: map[string]string{
			configMinInSyncReplicas: minInSyncReplicas,
		},
	}
}
