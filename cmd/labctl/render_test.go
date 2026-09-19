package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
)

func TestRenderTopicsNamesEverySettingThatDrifted(t *testing.T) {
	var out strings.Builder

	statuses := []kafka.TopicStatus{
		{Name: "payments.main", State: kafka.TopicCreated},
		{Name: "payments.dlq", State: kafka.TopicExists},
		{
			Name:  "payments.retry.5s",
			State: kafka.TopicDrifted,
			Drift: []kafka.TopicDrift{
				{Setting: "cleanup.policy", Want: "delete", Got: "compact"},
				{Setting: "partitions", Want: "6", Got: "1"},
				{Setting: "segment.bytes", Want: "1073741824", Got: ""},
			},
		},
	}

	if err := renderTopics(&out, statuses); err != nil {
		t.Fatalf("renderTopics: %v", err)
	}

	want := strings.Join([]string{
		"TOPIC              STATE    DRIFT",
		"payments.main      created  -",
		"payments.dlq       exists   -",
		"payments.retry.5s  drifted  cleanup.policy: want delete, got compact; partitions: want 6, got 1; segment.bytes: want 1073741824, got unset",
		"",
	}, "\n")
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Errorf("table mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderPartitionsFlagsUnderReplicationAndLeaderlessPartitions(t *testing.T) {
	var out strings.Builder

	states := []kafka.PartitionState{
		{Topic: "payments.main", Partition: 0, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3, 1}},
		{Topic: "payments.main", Partition: 1, Leader: 1, Replicas: []int32{3, 1, 2}, ISR: []int32{1, 2}},
		{Topic: "payments.main", Partition: 2, Leader: -1, Replicas: []int32{1, 2, 3}, ISR: nil},
		{Topic: "payments.main", Partition: 3, Leader: -1, Err: errors.New("LEADER_NOT_AVAILABLE")},
	}

	if err := renderPartitions(&out, states); err != nil {
		t.Fatalf("renderPartitions: %v", err)
	}

	want := strings.Join([]string{
		"TOPIC          PARTITION  LEADER  REPLICAS  ISR    URP  STATUS",
		"payments.main  0          2       2,3,1     2,3,1  -    -",
		"payments.main  1          1       3,1,2     1,2    yes  -",
		"payments.main  2          none    1,2,3     -      yes  -",
		"payments.main  3          none    -         -      ?    LEADER_NOT_AVAILABLE",
		"",
	}, "\n")
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Errorf("table mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTopicsPrintsNothingWhenTheClusterAnsweredNothing(t *testing.T) {
	var out strings.Builder

	if err := renderTopics(&out, nil); err != nil {
		t.Fatalf("renderTopics: %v", err)
	}
	if out.String() != "" {
		t.Errorf("output = %q, want empty", out.String())
	}
}

func TestRenderLagMarksPartitionsWhoseLagCouldNotBeComputed(t *testing.T) {
	var out strings.Builder

	lags := []kafka.GroupPartitionLag{
		{Group: "payments-consumer", Topic: "payments.main", Partition: 0, Committed: 1200, End: 1200, Lag: 0},
		{Group: "payments-consumer", Topic: "payments.main", Partition: 1, Committed: 900, End: 1500, Lag: 600},
		{Group: "payments-consumer", Topic: "payments.main", Partition: 2, Committed: -1, End: 1500, Lag: -1, Err: errors.New("GROUP_SUBSCRIBED_TO_TOPIC")},
	}

	if err := renderLag(&out, lags); err != nil {
		t.Fatalf("renderLag: %v", err)
	}

	want := strings.Join([]string{
		"GROUP              TOPIC          PARTITION  COMMITTED  END   LAG  STATUS",
		"payments-consumer  payments.main  0          1200       1200  0    -",
		"payments-consumer  payments.main  1          900        1500  600  -",
		"payments-consumer  payments.main  2          -1         1500  ?    GROUP_SUBSCRIBED_TO_TOPIC",
		"",
	}, "\n")
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Errorf("table mismatch (-want +got):\n%s", diff)
	}
}
