package kafka

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/twmb/franz-go/pkg/kgo"
)

// failures counts what the consumer reports as failed, by stage.
type failures struct {
	stages []string
}

func (f *failures) Handled(string)                     {}
func (f *failures) HandlingTook(string, time.Duration) {}
func (f *failures) Failed(stage string)                { f.stages = append(f.stages, stage) }

var errBrokerSaid = errors.New("NOT_AUTHORIZED")

func fetchWith(errs ...error) kgo.Fetches {
	partitions := make([]kgo.FetchPartition, 0, len(errs))
	for i, err := range errs {
		partitions = append(partitions, kgo.FetchPartition{Partition: int32(i), Err: err})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "payments.main", Partitions: partitions}}}}
}

// franz-go documents a lost group session and data loss as informational: the client rejoins,
// or resets past the gap, by itself. Exiting for them would add a restart's back-off to an
// outage the client was already riding out; every other fetch error still stops the consumer,
// and a lost session also skips the poll's records, whose offsets the group would refuse.
func TestOnlyTheFetchErrorsTheClientRecoversFromAreRiddenOut(t *testing.T) {
	type want struct {
		SessionLost bool
		Fatal       error
		Failed      []string
	}
	tests := []struct {
		name string
		args kgo.Fetches
		want want
	}{
		{
			name: "a lost group session",
			args: fetchWith(&kgo.ErrGroupSession{Err: errBrokerSaid}),
			want: want{SessionLost: true, Failed: []string{stageFetch}},
		},
		{
			name: "a log truncated under the consumer",
			args: fetchWith(&kgo.ErrDataLoss{Topic: "payments.main", ConsumedTo: 10, ResetTo: 5}),
			want: want{Failed: []string{stageFetch}},
		},
		{
			name: "an error the client does not recover from",
			args: fetchWith(errBrokerSaid),
			want: want{Fatal: errBrokerSaid},
		},
		{
			name: "a recoverable error beside a fatal one",
			args: fetchWith(&kgo.ErrDataLoss{Topic: "payments.main"}, errBrokerSaid),
			want: want{Fatal: errBrokerSaid, Failed: []string{stageFetch}},
		},
		{
			name: "nothing went wrong",
			args: fetchWith(),
			want: want{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			watch := &failures{}
			consumer := &Consumer{observer: watch, logger: slog.New(slog.DiscardHandler)}

			sessionLost, fatal := consumer.sortFetchErrors(t.Context(), tt.args)

			got := want{SessionLost: sessionLost, Fatal: fatal, Failed: watch.stages}
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}
		})
	}
}
