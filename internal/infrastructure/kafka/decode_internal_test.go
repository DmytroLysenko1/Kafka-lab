package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	paymentv1 "github.com/DmytroLysenko1/Kafka-lab/gen/payment/v1"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
)

const schemaID = 7

// A record is counted as the movement it says it is, and only that. The three messages share
// their field numbers, so the bytes of any one parse as any other: a capture read at the wrong
// index would move the authorised sum. A record without the event_type header is read by its
// message index alone, so what was published before the relay stamped the header stays readable.
func TestARecordIsReadAsTheMovementItSaysItIs(t *testing.T) {
	type args struct {
		index     []int
		eventType string
	}
	movement := func(kind merchant.Kind) merchants.PaymentEvent {
		return merchants.PaymentEvent{Kind: kind, EventID: "41", MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR"}
	}
	tests := []struct {
		name    string
		args    args
		want    merchants.PaymentEvent
		wantErr error
	}{
		{name: "an authorisation as the relay publishes it", args: args{index: []int{0}, eventType: "payment.authorized"}, want: movement(merchant.KindAuthorized)},
		{name: "a capture as the relay publishes it", args: args{index: []int{1}, eventType: "payment.captured"}, want: movement(merchant.KindCaptured)},
		{name: "a refund as the relay publishes it", args: args{index: []int{2}, eventType: "payment.refunded"}, want: movement(merchant.KindRefunded)},
		{name: "no event_type header, published before the relay set one", args: args{index: []int{0}}, want: movement(merchant.KindAuthorized)},
		{name: "a capture that says it is an authorisation", args: args{index: []int{1}, eventType: "payment.authorized"}, wantErr: ErrDecode},
		{name: "an authorisation that says it is a refund", args: args{index: []int{0}, eventType: "payment.refunded"}, wantErr: ErrDecode},
		{name: "a message past the last one this version knows", args: args{index: []int{3}}, wantErr: ErrDecode},
		{name: "a nested message index", args: args{index: []int{0, 1}}, wantErr: ErrDecode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := movementRecord(t, tt.args.index, tt.args.eventType)

			got, err := decodeEvent(record)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("event (-want +got):\n%s", diff)
			}
		})
	}
}

// The histogram hears every attempt, labelled by what it came to: the time a contended
// record spent waiting for its lock, or a failed one for the database, is exactly what an
// operator looks for, and a histogram of the successes alone would never show it.
func TestEveryAttemptAtARecordIsNamedByWhatItCameTo(t *testing.T) {
	type args struct {
		counted bool
		err     error
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "counted", args: args{counted: true}, want: outcomeCounted},
		{name: "already in the inbox", args: args{counted: false}, want: outcomeDuplicate},
		{name: "the merchant's row was locked", args: args{err: errors.Join(merchants.ErrContended, errDatabase)}, want: outcomeContended},
		{name: "the database refused the values", args: args{err: errors.Join(merchants.ErrUnprocessable, errDatabase)}, want: outcomeRefused},
		{name: "the database failed", args: args{err: errDatabase}, want: outcomeFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, handlingOutcome(tt.args.counted, tt.args.err)); diff != "" {
				t.Errorf("outcome (-want +got):\n%s", diff)
			}
		})
	}
}

var errDatabase = errors.New("the database is not answering")

func movementRecord(t *testing.T, index []int, eventType string) *kgo.Record {
	t.Helper()
	body, err := proto.Marshal(&paymentv1.PaymentAuthorized{MerchantId: "m-42", AmountMinor: 1999, Currency: "EUR"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var header sr.ConfluentHeader
	value, err := header.AppendEncode(nil, schemaID, index)
	if err != nil {
		t.Fatalf("encode the wire header: %v", err)
	}
	headers := []kgo.RecordHeader{{Key: "event_id", Value: []byte("41")}}
	if eventType != "" {
		headers = append(headers, kgo.RecordHeader{Key: "event_type", Value: []byte(eventType)})
	}
	return &kgo.Record{Value: append(value, body...), Headers: headers}
}

// A stop is judged by the error, never by the context afterwards: a real failure that
// happens to coincide with a SIGTERM — or with a sibling stage cancelling the group — is
// still a failure, and a record the database could not take during a rollout is still
// reported; only an error the cancellation itself caused is the consumer being stopped.
func TestOnlyAnErrorTheCancellationCausedIsAStop(t *testing.T) {
	tests := []struct {
		name string
		args error
		want bool
	}{
		{name: "the poll cut short by the stop", args: context.Canceled, want: true},
		{name: "a query the stop cancelled, wrapped by the driver", args: fmt.Errorf("exec: %w", context.Canceled), want: true},
		{name: "the client already closed", args: kgo.ErrClientClosed, want: true},
		{name: "the database failed at the same moment", args: errDatabase, want: false},
		{name: "a commit whose outcome is unknown", args: errors.Join(errDatabase, context.DeadlineExceeded), want: false},
		{name: "handling ran out of its own time", args: context.DeadlineExceeded, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, stopping(tt.args)); diff != "" {
				t.Errorf("stopping(%v) (-want +got):\n%s", tt.args, diff)
			}
		})
	}
}
