package payment_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

var (
	authorizedAt = time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	capturedAt   = time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	refundedAt   = time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)
	actionAt     = time.Date(2026, 10, 4, 9, 0, 0, 0, time.FixedZone("EEST", 3*60*60))
)

// stored is a payment as the store hands it back: in a status, with the history that status
// implies, and with nothing left to announce.
func stored(t *testing.T, status payment.Status) *payment.Payment {
	t.Helper()
	var captured, refunded time.Time
	switch status {
	case payment.StatusRefunded:
		refunded = refundedAt
		fallthrough
	case payment.StatusCaptured:
		captured = capturedAt
	}
	history := payment.NewHistory(authorizedAt, captured, refunded)
	return payment.Reconstitute(payment.NewID(), merchant(t), eur(t, 1999), status, history)
}

// announced is what an event says, comparable across the event types.
type announced struct {
	Kind     string
	Merchant string
	Minor    int64
	At       time.Time
}

func announcements(events []payment.Event) []announced {
	said := make([]announced, 0, len(events))
	for _, event := range events {
		kind := "unknown"
		switch event.(type) {
		case *payment.Authorized:
			kind = "authorized"
		case *payment.Captured:
			kind = "captured"
		case *payment.Refunded:
			kind = "refunded"
		}
		said = append(said, announced{Kind: kind, Merchant: event.Merchant().String(), Minor: event.Amount().Minor(), At: event.OccurredAt()})
	}
	return said
}

// A payment moves authorized → captured → refunded and nowhere else. Every move that happens
// announces itself exactly once; repeating the move a payment already made is the caller
// retrying a request whose answer it lost, and changes nothing and announces nothing — a
// second Captured would count the same money twice downstream.
func TestAPaymentMovesForwardOnlyAndAnnouncesEachMoveOnce(t *testing.T) {
	type args struct {
		from   payment.Status
		action func(*payment.Payment, time.Time) error
	}
	type want struct {
		Status  string
		History payment.History
		Events  []announced
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "capturing an authorisation",
			args: args{from: payment.StatusAuthorized, action: (*payment.Payment).Capture},
			want: want{Status: "captured", History: payment.NewHistory(authorizedAt, actionAt.UTC(), time.Time{}),
				Events: []announced{{Kind: "captured", Merchant: "m-42", Minor: 1999, At: actionAt.UTC()}}},
		},
		{
			name: "capturing again is the same capture",
			args: args{from: payment.StatusCaptured, action: (*payment.Payment).Capture},
			want: want{Status: "captured", History: payment.NewHistory(authorizedAt, capturedAt, time.Time{}), Events: []announced{}},
		},
		{
			name:    "capturing what was refunded",
			args:    args{from: payment.StatusRefunded, action: (*payment.Payment).Capture},
			want:    want{Status: "refunded", History: payment.NewHistory(authorizedAt, capturedAt, refundedAt), Events: []announced{}},
			wantErr: payment.ErrInvalidTransition,
		},
		{
			name: "refunding a capture",
			args: args{from: payment.StatusCaptured, action: (*payment.Payment).Refund},
			want: want{Status: "refunded", History: payment.NewHistory(authorizedAt, capturedAt, actionAt.UTC()),
				Events: []announced{{Kind: "refunded", Merchant: "m-42", Minor: 1999, At: actionAt.UTC()}}},
		},
		{
			name: "refunding again is the same refund",
			args: args{from: payment.StatusRefunded, action: (*payment.Payment).Refund},
			want: want{Status: "refunded", History: payment.NewHistory(authorizedAt, capturedAt, refundedAt), Events: []announced{}},
		},
		{
			name:    "refunding money that was never taken",
			args:    args{from: payment.StatusAuthorized, action: (*payment.Payment).Refund},
			want:    want{Status: "authorized", History: payment.NewHistory(authorizedAt, time.Time{}, time.Time{}), Events: []announced{}},
			wantErr: payment.ErrInvalidTransition,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := stored(t, tt.args.from)

			err := tt.args.action(changed, actionAt)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}

			got := want{Status: changed.Status().String(), History: changed.History(), Events: announcements(changed.PullEvents())}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(payment.History{})); diff != "" {
				t.Errorf("payment after the move (-want +got):\n%s", diff)
			}
		})
	}
}

// The whole life of a payment, end to end: three statuses, three announcements, in order —
// and pulling the events hands each over once, so a store that saves twice cannot publish twice.
func TestAPaymentsLifeIsThreeAnnouncementsInOrder(t *testing.T) {
	life, err := payment.Authorize(payment.NewID(), merchant(t), eur(t, 1999), authorizedAt)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if err := life.Capture(capturedAt); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if err := life.Refund(refundedAt); err != nil {
		t.Fatalf("refund: %v", err)
	}

	want := []announced{
		{Kind: "authorized", Merchant: "m-42", Minor: 1999, At: authorizedAt},
		{Kind: "captured", Merchant: "m-42", Minor: 1999, At: capturedAt},
		{Kind: "refunded", Merchant: "m-42", Minor: 1999, At: refundedAt},
	}
	if diff := cmp.Diff(want, announcements(life.PullEvents())); diff != "" {
		t.Errorf("announcements (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]announced{}, announcements(life.PullEvents())); diff != "" {
		t.Errorf("events were handed over twice (-want +got):\n%s", diff)
	}
}
