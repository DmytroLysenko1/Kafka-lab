package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
)

var errTopicRefused = errors.New("the topic is not taking writes")

// answering is the use case reduced to what it answered.
type answering struct {
	counted bool
	err     error
	called  bool
}

func (a *answering) Execute(context.Context, *merchants.PaymentEvent) (bool, error) {
	a.called = true
	return a.counted, a.err
}

// destination is where a record went when it left the partition unhandled.
type destination struct {
	Topic   string
	Attempt int
	Class   Class
}

// routes remembers where each record was sent, and refuses when told to.
type routes struct {
	refuse error
	sent   []destination
}

func (r *routes) Retry(_ context.Context, _ *kgo.Record, topic string, attempt int) error {
	if r.refuse != nil {
		return r.refuse
	}
	r.sent = append(r.sent, destination{
		Topic:   topic,
		Attempt: attempt,
	})
	return nil
}

func (r *routes) DeadLetter(_ context.Context, _ *kgo.Record, _ error, class Class) error {
	if r.refuse != nil {
		return r.refuse
	}
	r.sent = append(r.sent, destination{
		Class: class,
	})
	return nil
}

// watched is what the stage's panels would have shown.
type watched struct {
	Outcomes     []string
	FailedStages []string
}

func (w *watched) Handled(outcome string) {
	w.Outcomes = append(w.Outcomes, outcome)
}

func (w *watched) HandlingTook(string, time.Duration) {}

func (w *watched) Failed(stage string) {
	w.FailedStages = append(w.FailedStages, stage)
}

var (
	mainTier = Stage{
		Topic: "payments.main",
		Group: "payments-consumer",
		Tier:  0,
		Next:  "payments-consumer.retry.5s",
	}
	lastTier = Stage{
		Topic: "payments-consumer.retry.10m",
		Group: "payments-consumer.retry.10m",
		Tier:  3,
	}
)

// Every way a record can leave handleRecord, and what each leaves behind: an error keeps the
// offset where it is, so only a record that must be fetched again may return one, and a record
// that leaves the partition unhandled must have landed somewhere a person can find it.
func TestEveryRecordEitherLandsSomewhereOrHoldsItsOffset(t *testing.T) {
	type args struct {
		stage   Stage
		record  func(*testing.T) *kgo.Record
		answer  answering
		refuses error
	}
	type want struct {
		sent    []destination
		watched watched
		handled bool
	}
	authorized := func(t *testing.T) *kgo.Record {
		return movementRecord(t, []int{0}, "")
	}

	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "bytes that are not an event are archived without reaching the use case",
			args: args{stage: mainTier, record: func(*testing.T) *kgo.Record { return &kgo.Record{Value: []byte("not framed")} }},
			want: want{sent: []destination{{Class: ClassUndecodable}}},
		},
		{
			name: "a counted event moves the offset and says so",
			args: args{stage: mainTier, record: authorized, answer: answering{counted: true}},
			want: want{watched: watched{Outcomes: []string{outcomeCounted}}, handled: true},
		},
		{
			name: "a duplicate moves the offset as a duplicate",
			args: args{stage: mainTier, record: authorized},
			want: want{watched: watched{Outcomes: []string{outcomeDuplicate}}, handled: true},
		},
		{
			name: "an event the use case refuses is archived as refused",
			args: args{stage: mainTier, record: authorized, answer: answering{err: merchants.ErrUnprocessable}},
			want: want{sent: []destination{{Class: ClassRefused}}, handled: true},
		},
		{
			name: "a contended event moves to the next tier with the next attempt number",
			args: args{stage: mainTier, record: authorized, answer: answering{err: merchants.ErrContended}},
			want: want{sent: []destination{{Topic: "payments-consumer.retry.5s", Attempt: 1}}, handled: true},
		},
		{
			name: "a contended event on the last tier is archived as exhausted",
			args: args{stage: lastTier, record: authorized, answer: answering{err: merchants.ErrContended}},
			want: want{sent: []destination{{Class: ClassExhausted}}, handled: true},
		},
		{
			name:    "a database that is not answering holds the offset and shows as a failure",
			args:    args{stage: mainTier, record: authorized, answer: answering{err: errDatabase}},
			want:    want{watched: watched{FailedStages: []string{stageHandle}}, handled: true},
			wantErr: errDatabase,
		},
		{
			name:    "a stop holds the offset without counting as a failure",
			args:    args{stage: mainTier, record: authorized, answer: answering{err: fmt.Errorf("exec: %w", context.Canceled)}},
			want:    want{handled: true},
			wantErr: context.Canceled,
		},
		{
			name:    "a dead letter topic that refuses keeps the record where it is",
			args:    args{stage: mainTier, record: authorized, answer: answering{err: merchants.ErrUnprocessable}, refuses: errTopicRefused},
			want:    want{watched: watched{FailedStages: []string{stageDeadLetter}}, handled: true},
			wantErr: ErrDeadLetter,
		},
		{
			name:    "a retry tier that refuses keeps the record where it is",
			args:    args{stage: mainTier, record: authorized, answer: answering{err: merchants.ErrContended}, refuses: errTopicRefused},
			want:    want{watched: watched{FailedStages: []string{stageRetry}}, handled: true},
			wantErr: ErrRetry,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			answer := tt.args.answer
			sent := &routes{refuse: tt.args.refuses}
			seen := &watched{}
			consumer := &Consumer{
				stage:    tt.args.stage,
				handle:   &answer,
				detours:  sent,
				logger:   slog.New(slog.DiscardHandler),
				observer: seen,
				tracer:   kotel.NewTracer(),
			}

			err := consumer.handleRecord(t.Context(), tt.args.record(t))

			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			got := want{sent: sent.sent, watched: *seen, handled: answer.called}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("handleRecord mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
