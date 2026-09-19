package kafka

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"

	"github.com/samber/lo"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kslog"
)

const (
	brokerRequestTimeoutMillis int32 = 10_000

	unsetValue = "unset"
)

var (
	ErrCluster    = errors.New("kafka cluster call failed")
	ErrTopicDrift = errors.New("topic exists with a shape the catalog does not declare")
	ErrNoTopics   = errors.New("no topics requested")
	ErrNoGroups   = errors.New("no consumer groups requested")
)

type Option func(*adminConfig)

type adminConfig struct {
	logger *slog.Logger
}

func WithLogger(logger *slog.Logger) Option {
	return func(config *adminConfig) {
		config.logger = logger
	}
}

type Admin struct {
	client *kadm.Client
}

func NewAdmin(seeds []string, opts ...Option) (*Admin, error) {
	var config adminConfig
	for _, opt := range opts {
		opt(&config)
	}

	clientOpts := []kgo.Opt{kgo.SeedBrokers(seeds...)}
	if config.logger != nil {
		clientOpts = append(clientOpts, kgo.WithLogger(kslog.New(config.logger)))
	}

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka.NewAdmin %v: %w", seeds, errors.Join(ErrCluster, err))
	}

	admin := kadm.NewClient(client)
	admin.SetTimeoutMillis(brokerRequestTimeoutMillis)

	return &Admin{
		client: admin,
	}, nil
}

func (a *Admin) Close() {
	a.client.Close()
}

type TopicState string

const (
	TopicCreated TopicState = "created"
	TopicExists  TopicState = "exists"
	TopicDrifted TopicState = "drifted"
)

type TopicDrift struct {
	Setting string
	Want    string
	Got     string
}

type TopicStatus struct {
	Name  string
	State TopicState
	Drift []TopicDrift
}

// EnsureTopics returns a status for every spec it managed to reach, even when some of
// them failed: a partial table plus the joined errors is more useful to an operator than
// an empty one.
func (a *Admin) EnsureTopics(ctx context.Context, specs []TopicSpec) ([]TopicStatus, error) {
	if len(specs) == 0 {
		return nil, ErrNoTopics
	}

	statuses := make(map[string]TopicStatus, len(specs))
	existing := make([]TopicSpec, 0, len(specs))
	failures := make([]error, 0, len(specs))

	for _, group := range groupByShape(specs) {
		created, alreadyThere, err := a.createGroup(ctx, group)
		maps.Copy(statuses, created)
		existing = append(existing, alreadyThere...)
		if err != nil {
			failures = append(failures, err)
		}
	}

	compared, err := a.compareExisting(ctx, existing)
	maps.Copy(statuses, compared)
	if err != nil {
		failures = append(failures, err)
	}

	ordered := orderStatuses(specs, statuses)
	if slices.ContainsFunc(ordered, func(status TopicStatus) bool {
		return status.State == TopicDrifted
	}) {
		failures = append(failures, fmt.Errorf("kafka.Admin.EnsureTopics: %w", ErrTopicDrift))
	}
	return ordered, errors.Join(failures...)
}

func (a *Admin) DeleteTopics(ctx context.Context, topics ...string) error {
	if len(topics) == 0 {
		return ErrNoTopics
	}

	responses, err := a.client.DeleteTopics(ctx, topics...)
	if err != nil {
		return fmt.Errorf("kafka.Admin.DeleteTopics %v: %w", topics, errors.Join(ErrCluster, err))
	}

	failures := make([]error, 0, len(topics))
	for _, response := range responses.Sorted() {
		if response.Err != nil {
			failures = append(failures, fmt.Errorf("kafka.Admin.DeleteTopics %s: %w", response.Topic, errors.Join(ErrCluster, response.Err)))
		}
	}
	return errors.Join(failures...)
}

func (a *Admin) AddPartitions(ctx context.Context, add int, topics ...string) error {
	if len(topics) == 0 {
		return ErrNoTopics
	}

	responses, err := a.client.CreatePartitions(ctx, add, topics...)
	if err != nil {
		return fmt.Errorf("kafka.Admin.AddPartitions %v: %w", topics, errors.Join(ErrCluster, err))
	}

	failures := make([]error, 0, len(topics))
	for _, response := range responses.Sorted() {
		if response.Err != nil {
			failures = append(failures, fmt.Errorf("kafka.Admin.AddPartitions %s: %w", response.Topic, errors.Join(ErrCluster, response.Err)))
		}
	}
	return errors.Join(failures...)
}

func (a *Admin) createGroup(ctx context.Context, group topicGroup) (map[string]TopicStatus, []TopicSpec, error) {
	responses, err := a.client.CreateTopics(
		ctx, group.shape.Partitions,
		group.shape.ReplicationFactor, topicConfig(group.shape.Config),
		group.names...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("kafka.Admin.EnsureTopics %v: %w", group.names, errors.Join(ErrCluster, err))
	}

	created := make(map[string]TopicStatus, len(group.names))
	existing := make([]TopicSpec, 0, len(group.names))
	failures := make([]error, 0, len(group.names))
	for _, name := range group.names {
		response, ok := responses[name]
		switch {
		case !ok:
			failures = append(failures, fmt.Errorf("kafka.Admin.EnsureTopics %s: no broker response: %w", name, ErrCluster))
		case errors.Is(response.Err, kerr.TopicAlreadyExists):
			existing = append(existing, group.spec(name))
		case response.Err != nil:
			failures = append(failures, fmt.Errorf("kafka.Admin.EnsureTopics %s: %w", name, errors.Join(ErrCluster, response.Err)))
		default:
			created[name] = TopicStatus{
				Name:  name,
				State: TopicCreated,
			}
		}
	}
	return created, existing, errors.Join(failures...)
}

func (a *Admin) compareExisting(ctx context.Context, specs []TopicSpec) (map[string]TopicStatus, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	names := lo.Map(specs, func(spec TopicSpec, _ int) string {
		return spec.Name
	})

	details, err := a.client.ListTopics(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("kafka.Admin.EnsureTopics list %v: %w", names, errors.Join(ErrCluster, err))
	}
	resources, err := a.client.DescribeTopicConfigs(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("kafka.Admin.EnsureTopics configs %v: %w", names, errors.Join(ErrCluster, err))
	}

	statuses := make(map[string]TopicStatus, len(specs))
	failures := make([]error, 0, len(specs))
	for _, spec := range specs {
		detail, ok := details[spec.Name]
		if !ok || detail.Err != nil {
			failures = append(failures, fmt.Errorf("kafka.Admin.EnsureTopics %s: metadata unavailable: %w", spec.Name, errors.Join(ErrCluster, detail.Err)))
			continue
		}
		actual, err := configValues(resources, spec.Name)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		drift := driftOf(spec, &detail, actual)
		statuses[spec.Name] = TopicStatus{
			Name:  spec.Name,
			State: stateOf(drift),
			Drift: drift,
		}
	}
	return statuses, errors.Join(failures...)
}

type PartitionState struct {
	Topic     string
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
	Err       error
}

// Known reports whether the broker actually described this partition. A partition that
// failed to load carries its error and nothing else, and an empty replica list must not
// read as a healthy one.
func (p *PartitionState) Known() bool {
	return p.Err == nil && len(p.Replicas) > 0
}

func (p *PartitionState) UnderReplicated() bool {
	return p.Known() && len(p.ISR) < len(p.Replicas)
}

// Status is the broker's own name for why this partition could not be described, so the
// caller can print it without importing the client's error types.
func (p *PartitionState) Status() string {
	if p.Err == nil {
		return ""
	}
	if broker, ok := errors.AsType[*kerr.Error](p.Err); ok {
		return broker.Message
	}
	return p.Err.Error()
}

func (a *Admin) Describe(ctx context.Context, topics ...string) ([]PartitionState, error) {
	if len(topics) == 0 {
		return nil, ErrNoTopics
	}

	details, err := a.listTopics(ctx, topics)
	if err != nil {
		return nil, err
	}

	partitions := 0
	for _, detail := range details {
		partitions += len(detail.Partitions)
	}

	states := make([]PartitionState, 0, partitions)
	details.EachPartition(func(partition kadm.PartitionDetail) {
		states = append(states, PartitionState{
			Topic:     partition.Topic,
			Partition: partition.Partition,
			Leader:    partition.Leader,
			Replicas:  partition.Replicas,
			ISR:       partition.ISR,
			Err:       partition.Err,
		})
	})
	slices.SortFunc(states, func(left, right PartitionState) int {
		return cmp.Or(
			cmp.Compare(left.Topic, right.Topic),
			cmp.Compare(left.Partition, right.Partition),
		)
	})
	return states, nil
}

type GroupPartitionLag struct {
	Group     string
	Topic     string
	Partition int32
	Committed int64
	End       int64
	Lag       int64
	Err       error
}

func (a *Admin) Lag(ctx context.Context, groups ...string) ([]GroupPartitionLag, error) {
	if len(groups) == 0 {
		return nil, ErrNoGroups
	}

	described, err := a.client.Lag(ctx, groups...)
	if err != nil {
		return nil, fmt.Errorf("kafka.Admin.Lag %v: %w", groups, errors.Join(ErrCluster, err))
	}

	sorted := described.Sorted()
	lags := make([]GroupPartitionLag, 0, len(sorted))
	failures := make([]error, 0, len(sorted))
	for i := range sorted {
		group := &sorted[i]
		if err := errors.Join(group.DescribeErr, group.FetchErr); err != nil {
			failures = append(failures, fmt.Errorf("kafka.Admin.Lag %s: %w", group.Group, errors.Join(ErrCluster, err)))
			continue
		}
		rows, rowFailures := partitionLags(group)
		lags = append(lags, rows...)
		failures = append(failures, rowFailures...)
	}
	return lags, errors.Join(failures...)
}

// partitionLags flattens one group's lag in topic and partition order. A partition whose
// lag could not be computed stays in the table and is also returned as a failure, so an
// unknown lag can never leave the command with a zero exit status.
func partitionLags(group *kadm.DescribedGroupLag) ([]GroupPartitionLag, []error) {
	var (
		rows     []GroupPartitionLag
		failures []error
	)
	for _, topic := range slices.Sorted(maps.Keys(group.Lag)) {
		partitions := group.Lag[topic]
		for _, number := range slices.Sorted(maps.Keys(partitions)) {
			member := partitions[number]
			if member.Err != nil {
				failures = append(failures, fmt.Errorf("kafka.Admin.Lag %s %s/%d: %w",
					group.Group, member.Topic, member.Partition, errors.Join(ErrCluster, member.Err)))
			}
			rows = append(rows, GroupPartitionLag{
				Group:     group.Group,
				Topic:     member.Topic,
				Partition: member.Partition,
				Committed: member.Commit.At,
				End:       member.End.Offset,
				Lag:       member.Lag,
				Err:       member.Err,
			})
		}
	}
	return rows, failures
}

func (a *Admin) listTopics(ctx context.Context, topics []string) (kadm.TopicDetails, error) {
	details, err := a.client.ListTopics(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka.Admin list %v: %w", topics, errors.Join(ErrCluster, err))
	}
	if err := details.Error(); err != nil {
		return nil, fmt.Errorf("kafka.Admin list %v: %w", topics, errors.Join(ErrCluster, err))
	}
	return details, nil
}

type topicGroup struct {
	shape TopicSpec
	names []string
}

func (g topicGroup) spec(name string) TopicSpec {
	spec := g.shape
	spec.Name = name
	return spec
}

func groupByShape(specs []TopicSpec) []topicGroup {
	groups := make([]topicGroup, 0, len(specs))

	for _, spec := range specs {
		at := slices.IndexFunc(groups, func(group topicGroup) bool {
			return sameShape(group.shape, spec)
		})
		if at < 0 {
			groups = append(groups, topicGroup{
				shape: spec,
				names: []string{spec.Name},
			})
			continue
		}
		groups[at].names = append(groups[at].names, spec.Name)
	}
	return groups
}

func sameShape(left, right TopicSpec) bool {
	return left.Partitions == right.Partitions &&
		left.ReplicationFactor == right.ReplicationFactor &&
		maps.Equal(left.Config, right.Config)
}

type topicConfigs struct {
	effective map[string]string
	overrides map[string]string
}

func driftOf(spec TopicSpec, detail *kadm.TopicDetail, actual topicConfigs) []TopicDrift {
	drift := make([]TopicDrift, 0, len(spec.Config)+len(actual.overrides)+2)

	if got := len(detail.Partitions); got != int(spec.Partitions) {
		drift = append(drift, TopicDrift{
			Setting: "partitions",
			Want:    strconv.Itoa(int(spec.Partitions)),
			Got:     strconv.Itoa(got),
		})
	}
	if got := replicationFactor(detail); got != int(spec.ReplicationFactor) {
		drift = append(drift, TopicDrift{
			Setting: "replication factor",
			Want:    strconv.Itoa(int(spec.ReplicationFactor)),
			Got:     strconv.Itoa(got),
		})
	}
	for setting, want := range spec.Config {
		if got := actual.effective[setting]; got != want {
			drift = append(drift, TopicDrift{
				Setting: setting,
				Want:    want,
				Got:     got,
			})
		}
	}
	for setting, got := range actual.overrides {
		if _, declared := spec.Config[setting]; !declared {
			drift = append(drift, TopicDrift{
				Setting: setting,
				Want:    unsetValue,
				Got:     got,
			})
		}
	}

	slices.SortFunc(drift, func(left, right TopicDrift) int {
		return cmp.Compare(left.Setting, right.Setting)
	})
	return drift
}

func replicationFactor(detail *kadm.TopicDetail) int {
	factor := 0
	for _, partition := range detail.Partitions {
		if factor == 0 || len(partition.Replicas) < factor {
			factor = len(partition.Replicas)
		}
	}
	return factor
}

func stateOf(drift []TopicDrift) TopicState {
	if len(drift) == 0 {
		return TopicExists
	}
	return TopicDrifted
}

func orderStatuses(specs []TopicSpec, statuses map[string]TopicStatus) []TopicStatus {
	return lo.FilterMap(specs, func(spec TopicSpec, _ int) (TopicStatus, bool) {
		status, ok := statuses[spec.Name]
		return status, ok
	})
}

func configValues(resources kadm.ResourceConfigs, topic string) (topicConfigs, error) {
	for _, resource := range resources {
		if resource.Name != topic {
			continue
		}
		if resource.Err != nil {
			return topicConfigs{}, fmt.Errorf("kafka.Admin.EnsureTopics configs %s: %w", topic, errors.Join(ErrCluster, resource.Err))
		}

		overrides := lo.Filter(resource.Configs, func(config kadm.Config, _ int) bool {
			return config.Source == kmsg.ConfigSourceDynamicTopicConfig
		})
		return topicConfigs{
			effective: lo.SliceToMap(resource.Configs, configEntry),
			overrides: lo.SliceToMap(overrides, configEntry),
		}, nil
	}
	return topicConfigs{}, fmt.Errorf("kafka.Admin.EnsureTopics configs %s: no broker response: %w", topic, ErrCluster)
}

func configEntry(config kadm.Config) (string, string) {
	return config.Key, lo.FromPtr(config.Value)
}

func topicConfig(config map[string]string) map[string]*string {
	return lo.MapValues(config, func(value string, _ string) *string {
		return new(value)
	})
}
