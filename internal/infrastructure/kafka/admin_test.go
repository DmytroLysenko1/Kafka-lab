package kafka_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"
	"github.com/twmb/franz-go/pkg/kfake"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
)

const clusterBrokers = 3

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestAdminEnsureTopicsCreatesTheCatalogAndAcceptsItOnEveryLaterRun(t *testing.T) {
	admin := newTestAdmin(t)
	specs := []kafka.TopicSpec{
		{
			Name:              "payments.main",
			Partitions:        2,
			ReplicationFactor: clusterBrokers,
			Config:            map[string]string{"min.insync.replicas": "2"},
		},
		{
			Name:              "payments.dlq",
			Partitions:        1,
			ReplicationFactor: clusterBrokers,
			Config:            map[string]string{"retention.ms": "604800000"},
		},
	}

	firstRun, err := admin.EnsureTopics(t.Context(), specs)
	if err != nil {
		t.Fatalf("first EnsureTopics: %v", err)
	}
	want := []kafka.TopicStatus{
		{Name: "payments.main", State: kafka.TopicCreated},
		{Name: "payments.dlq", State: kafka.TopicCreated},
	}
	if diff := cmp.Diff(want, firstRun); diff != "" {
		t.Errorf("first run mismatch (-want +got):\n%s", diff)
	}

	secondRun, err := admin.EnsureTopics(t.Context(), specs)
	if err != nil {
		t.Fatalf("second EnsureTopics: %v", err)
	}
	want = []kafka.TopicStatus{
		{Name: "payments.main", State: kafka.TopicExists, Drift: []kafka.TopicDrift{}},
		{Name: "payments.dlq", State: kafka.TopicExists, Drift: []kafka.TopicDrift{}},
	}
	if diff := cmp.Diff(want, secondRun); diff != "" {
		t.Errorf("second run mismatch (-want +got):\n%s", diff)
	}
}

func TestAdminEnsureTopicsRefusesToRunOnATopicAnExperimentReshaped(t *testing.T) {
	admin := newTestAdmin(t)
	reshaped := []kafka.TopicSpec{{
		Name:              "payments.main",
		Partitions:        1,
		ReplicationFactor: 1,
		Config:            map[string]string{"min.insync.replicas": "1"},
	}}
	if _, err := admin.EnsureTopics(t.Context(), reshaped); err != nil {
		t.Fatalf("seeding the reshaped topic: %v", err)
	}

	catalog := []kafka.TopicSpec{{
		Name:              "payments.main",
		Partitions:        6,
		ReplicationFactor: clusterBrokers,
		Config:            map[string]string{"min.insync.replicas": "2"},
	}}

	got, err := admin.EnsureTopics(t.Context(), catalog)
	if !errors.Is(err, kafka.ErrTopicDrift) {
		t.Fatalf("err = %v, want %v", err, kafka.ErrTopicDrift)
	}
	want := []kafka.TopicStatus{{
		Name:  "payments.main",
		State: kafka.TopicDrifted,
		Drift: []kafka.TopicDrift{
			{Setting: "min.insync.replicas", Want: "2", Got: "1"},
			{Setting: "partitions", Want: "6", Got: "1"},
			{Setting: "replication factor", Want: "3", Got: "1"},
		},
	}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("drift mismatch (-want +got):\n%s", diff)
	}
}

func TestAdminDescribeListsEveryPartitionOrderedAndFullyReplicated(t *testing.T) {
	admin := newTestAdmin(t)
	if _, err := admin.EnsureTopics(t.Context(), []kafka.TopicSpec{
		{Name: "payments.retry", Partitions: 1, ReplicationFactor: clusterBrokers},
		{Name: "payments.main", Partitions: 3, ReplicationFactor: clusterBrokers},
	}); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}

	got, err := admin.Describe(t.Context(), "payments.main", "payments.retry")
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}

	type row struct {
		Topic     string
		Partition int32
	}
	wantOrder := []row{
		{Topic: "payments.main", Partition: 0},
		{Topic: "payments.main", Partition: 1},
		{Topic: "payments.main", Partition: 2},
		{Topic: "payments.retry", Partition: 0},
	}
	gotOrder := lo.Map(got, func(state kafka.PartitionState, _ int) row {
		return row{Topic: state.Topic, Partition: state.Partition}
	})
	if diff := cmp.Diff(wantOrder, gotOrder); diff != "" {
		t.Errorf("rows mismatch (-want +got):\n%s", diff)
	}

	for _, state := range got {
		if len(state.Replicas) != clusterBrokers {
			t.Errorf("%s/%d: replicas = %v, want %d of them", state.Topic, state.Partition, state.Replicas, clusterBrokers)
		}
		if diff := cmp.Diff(slices.Sorted(slices.Values(state.Replicas)), slices.Sorted(slices.Values(state.ISR))); diff != "" {
			t.Errorf("%s/%d: ISR differs from replicas on an intact cluster (-replicas +isr):\n%s", state.Topic, state.Partition, diff)
		}
		if state.UnderReplicated() {
			t.Errorf("%s/%d: reported under-replicated on an intact cluster", state.Topic, state.Partition)
		}
	}
}

func TestAdminDescribeFailsClosed(t *testing.T) {
	admin := newTestAdmin(t)

	tests := []struct {
		name    string
		topics  []string
		wantErr error
	}{
		{
			name:    "an unknown topic is an error, not an empty table",
			topics:  []string{"payments.absent"},
			wantErr: kafka.ErrCluster,
		},
		{
			name:    "no topics is an error, not a dump of the whole cluster",
			topics:  nil,
			wantErr: kafka.ErrNoTopics,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := admin.Describe(t.Context(), tt.topics...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != nil {
				t.Errorf("states = %v, want none", got)
			}
		})
	}
}

func TestAdminEnsureTopicsSeesAnOverrideTheCatalogNeverDeclared(t *testing.T) {
	admin := newTestAdmin(t)
	reshaped := []kafka.TopicSpec{{
		Name:              "payments.dlq",
		Partitions:        1,
		ReplicationFactor: clusterBrokers,
		Config:            map[string]string{"cleanup.policy": "compact"},
	}}
	if _, err := admin.EnsureTopics(t.Context(), reshaped); err != nil {
		t.Fatalf("seeding the reshaped topic: %v", err)
	}

	catalog := []kafka.TopicSpec{{
		Name:              "payments.dlq",
		Partitions:        1,
		ReplicationFactor: clusterBrokers,
	}}

	got, err := admin.EnsureTopics(t.Context(), catalog)
	if !errors.Is(err, kafka.ErrTopicDrift) {
		t.Fatalf("err = %v, want %v — a dead-letter topic silently turned into a compacted one", err, kafka.ErrTopicDrift)
	}
	want := []kafka.TopicStatus{{
		Name:  "payments.dlq",
		State: kafka.TopicDrifted,
		Drift: []kafka.TopicDrift{{Setting: "cleanup.policy", Want: "unset", Got: "compact"}},
	}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("drift mismatch (-want +got):\n%s", diff)
	}
}

func TestAdminEnsureTopicsStillReportsTheTopicsAroundOneItCouldNotCreate(t *testing.T) {
	admin := newTestAdmin(t)
	specs := []kafka.TopicSpec{
		{Name: "payments.main", Partitions: 1, ReplicationFactor: clusterBrokers},
		{Name: "payments.overreplicated", Partitions: 1, ReplicationFactor: clusterBrokers + 2},
		{Name: "payments.dlq", Partitions: 1, ReplicationFactor: clusterBrokers},
	}

	got, err := admin.EnsureTopics(t.Context(), specs)
	if !errors.Is(err, kafka.ErrCluster) {
		t.Fatalf("err = %v, want %v for the topic no cluster of %d brokers can host", err, kafka.ErrCluster, clusterBrokers)
	}
	want := []kafka.TopicStatus{
		{Name: "payments.main", State: kafka.TopicCreated},
		{Name: "payments.dlq", State: kafka.TopicCreated},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("the failure hid the topics that were created (-want +got):\n%s", diff)
	}
}

func TestAdminDeleteTopicsClearsTheWayForRecreatingTheCatalog(t *testing.T) {
	admin := newTestAdmin(t)
	catalog := []kafka.TopicSpec{{
		Name:              "payments.main",
		Partitions:        1,
		ReplicationFactor: clusterBrokers,
	}}
	if _, err := admin.EnsureTopics(t.Context(), catalog); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}

	if err := admin.AddPartitions(t.Context(), 2, "payments.main"); err != nil {
		t.Fatalf("AddPartitions: %v", err)
	}
	reshaped, err := admin.EnsureTopics(t.Context(), catalog)
	if !errors.Is(err, kafka.ErrTopicDrift) {
		t.Fatalf("after adding partitions err = %v, want %v", err, kafka.ErrTopicDrift)
	}
	wantDrift := []kafka.TopicDrift{{Setting: "partitions", Want: "1", Got: "3"}}
	if diff := cmp.Diff(wantDrift, reshaped[0].Drift); diff != "" {
		t.Errorf("drift mismatch (-want +got):\n%s", diff)
	}

	if err := admin.DeleteTopics(t.Context(), "payments.main"); err != nil {
		t.Fatalf("DeleteTopics: %v", err)
	}
	recreated, err := admin.EnsureTopics(t.Context(), catalog)
	if err != nil {
		t.Fatalf("EnsureTopics after delete: %v", err)
	}
	want := []kafka.TopicStatus{{Name: "payments.main", State: kafka.TopicCreated}}
	if diff := cmp.Diff(want, recreated); diff != "" {
		t.Errorf("recreated mismatch (-want +got):\n%s", diff)
	}
}

func TestPartitionStateNeverReportsAnUndescribedPartitionAsHealthy(t *testing.T) {
	tests := []struct {
		name                string
		state               kafka.PartitionState
		wantKnown           bool
		wantUnderReplicated bool
	}{
		{
			name:      "a fully replicated partition",
			state:     kafka.PartitionState{Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
			wantKnown: true,
		},
		{
			name:                "a partition whose ISR shrank",
			state:               kafka.PartitionState{Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2}},
			wantKnown:           true,
			wantUnderReplicated: true,
		},
		{
			name:  "a partition the broker could not describe carries no replicas and must not read as healthy",
			state: kafka.PartitionState{Leader: -1, Err: errors.New("LEADER_NOT_AVAILABLE")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.Known(); got != tt.wantKnown {
				t.Errorf("Known() = %t, want %t", got, tt.wantKnown)
			}
			if got := tt.state.UnderReplicated(); got != tt.wantUnderReplicated {
				t.Errorf("UnderReplicated() = %t, want %t", got, tt.wantUnderReplicated)
			}
		})
	}
}

func newTestAdmin(t *testing.T) *kafka.Admin {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(clusterBrokers))
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	admin, err := kafka.NewAdmin(cluster.ListenAddrs())
	if err != nil {
		t.Fatalf("kafka.NewAdmin: %v", err)
	}
	t.Cleanup(admin.Close)

	return admin
}
