package payments

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

const (
	paymentUnderTest = "11111111-1111-4111-8111-111111111111"
	merchantOfIt     = "m-42"
)

var changedAt = time.Date(2026, time.October, 5, 12, 0, 0, 0, time.UTC)

// row is a payments row and the outbox rows written with it, as the fake keeps them.
type row struct {
	Merchant string
	Status   string
	History  payment.History
	Outbox   []string
}

// changeDB keeps rows the way the real store does: a read is scoped to the merchant, a write
// applies only if the status is still the one it was decided from, and nothing lands unless the
// transaction commits — so a test can tell a rolled-back change from one that never happened.
type changeDB struct {
	rows       map[string]row
	staged     []func()
	failSave   error
	failCommit error
}

func (db *changeDB) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	db.staged = nil
	defer func() { db.staged = nil }()
	if err := fn(ctx); err != nil {
		return err
	}
	if db.failCommit != nil {
		return db.failCommit
	}
	for _, apply := range db.staged {
		apply()
	}
	return nil
}

func (db *changeDB) FindForMerchantForUpdate(_ context.Context, id payment.ID, merchant payment.MerchantID) (*payment.Payment, error) {
	stored, found := db.rows[id.String()]
	if !found || stored.Merchant != merchant.String() {
		return nil, payment.ErrNotFound
	}
	status := statusNamed(stored.Status)
	amount, err := payment.ParseMoney(1999, "EUR")
	if err != nil {
		return nil, err
	}
	return payment.Reconstitute(id, merchant, amount, status, stored.History), nil
}

func (db *changeDB) Save(_ context.Context, changed *payment.Payment, from payment.Status) error {
	if db.failSave != nil {
		return db.failSave
	}
	id := changed.ID().String()
	if db.rows[id].Status != from.String() {
		return payment.ErrConcurrentModification
	}
	written := row{Merchant: changed.Merchant().String(), Status: changed.Status().String(), History: changed.History(), Outbox: db.rows[id].Outbox}
	for _, event := range changed.PullEvents() {
		written.Outbox = append(written.Outbox, eventName(event))
	}
	db.staged = append(db.staged, func() { db.rows[id] = written })
	return nil
}

func eventName(event payment.Event) string {
	switch event.(type) {
	case *payment.Captured:
		return "captured"
	case *payment.Refunded:
		return "refunded"
	}
	return "other"
}

func dbWith(status string) *changeDB {
	var captured, refunded time.Time
	if status == "captured" || status == "refunded" {
		captured = changedAt.Add(-24 * time.Hour)
	}
	if status == "refunded" {
		refunded = changedAt.Add(-time.Hour)
	}
	history := payment.NewHistory(changedAt.Add(-48*time.Hour), captured, refunded)
	return &changeDB{rows: map[string]row{
		paymentUnderTest: {Merchant: merchantOfIt, Status: status, History: history, Outbox: []string{}},
	}}
}

type changeUseCase interface {
	Execute(ctx context.Context, cmd ChangeCommand) (ChangeResult, error)
}

func capture(db *changeDB) changeUseCase {
	return NewCapturePayment(db, db, func() time.Time { return changedAt })
}

func refund(db *changeDB) changeUseCase {
	return NewRefundPayment(db, db, func() time.Time { return changedAt })
}

// What a capture or a refund leaves behind is the payment's status and the events stored with
// it. A move that is refused must leave both as they were; a move repeated must leave the
// status and add no event, or the same money would be counted twice downstream.
func TestAChangeLeavesThePaymentAndItsEventsAsTheMoveSays(t *testing.T) {
	type args struct {
		from    string
		useCase func(*changeDB) changeUseCase
		cmd     ChangeCommand
	}
	type want struct {
		Result ChangeResult
		Row    row
	}
	ofIt := ChangeCommand{MerchantID: merchantOfIt, PaymentID: paymentUnderTest}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "capture an authorisation",
			args: args{from: "authorized", useCase: capture, cmd: ofIt},
			want: want{Result: ChangeResult{PaymentID: paymentUnderTest, Status: "captured"},
				Row: row{Merchant: merchantOfIt, Status: "captured", Outbox: []string{"captured"}}},
		},
		{
			name: "capture repeated after its answer was lost",
			args: args{from: "captured", useCase: capture, cmd: ofIt},
			want: want{Result: ChangeResult{PaymentID: paymentUnderTest, Status: "captured"},
				Row: row{Merchant: merchantOfIt, Status: "captured", Outbox: []string{}}},
		},
		{
			name:    "capture what was refunded",
			args:    args{from: "refunded", useCase: capture, cmd: ofIt},
			want:    want{Row: row{Merchant: merchantOfIt, Status: "refunded", Outbox: []string{}}},
			wantErr: payment.ErrInvalidTransition,
		},
		{
			name: "refund a capture",
			args: args{from: "captured", useCase: refund, cmd: ofIt},
			want: want{Result: ChangeResult{PaymentID: paymentUnderTest, Status: "refunded"},
				Row: row{Merchant: merchantOfIt, Status: "refunded", Outbox: []string{"refunded"}}},
		},
		{
			name:    "refund money that was never taken",
			args:    args{from: "authorized", useCase: refund, cmd: ofIt},
			want:    want{Row: row{Merchant: merchantOfIt, Status: "authorized", Outbox: []string{}}},
			wantErr: payment.ErrInvalidTransition,
		},
		{
			name:    "capture another merchant's payment",
			args:    args{from: "authorized", useCase: capture, cmd: ChangeCommand{MerchantID: "m-7", PaymentID: paymentUnderTest}},
			want:    want{Row: row{Merchant: merchantOfIt, Status: "authorized", Outbox: []string{}}},
			wantErr: payment.ErrNotFound,
		},
		{
			name:    "capture a payment id nobody could have",
			args:    args{from: "authorized", useCase: capture, cmd: ChangeCommand{MerchantID: merchantOfIt, PaymentID: "not-a-uuid"}},
			want:    want{Row: row{Merchant: merchantOfIt, Status: "authorized", Outbox: []string{}}},
			wantErr: payment.ErrNotFound,
		},
		{
			name:    "capture for no merchant",
			args:    args{from: "authorized", useCase: capture, cmd: ChangeCommand{PaymentID: paymentUnderTest}},
			want:    want{Row: row{Merchant: merchantOfIt, Status: "authorized", Outbox: []string{}}},
			wantErr: payment.ErrMerchantRequired,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := dbWith(tt.args.from)

			got, err := tt.args.useCase(db).Execute(t.Context(), tt.args.cmd)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			stored := db.rows[paymentUnderTest]
			stored.History = payment.History{}
			if diff := cmp.Diff(tt.want, want{Result: got, Row: stored}, cmp.AllowUnexported(payment.History{})); diff != "" {
				t.Errorf("after the change (-want +got):\n%s", diff)
			}
		})
	}
}

// A capture is stored with the time it happened, which is the time the payment and its event
// both carry: a captured_at that disagreed with the event's occurred_at would be two answers to
// one question.
func TestACaptureIsStoredWithTheTimeItHappened(t *testing.T) {
	db := dbWith("authorized")

	if _, err := capture(db).Execute(t.Context(), ChangeCommand{MerchantID: merchantOfIt, PaymentID: paymentUnderTest}); err != nil {
		t.Fatalf("capture: %v", err)
	}

	if diff := cmp.Diff(changedAt, db.rows[paymentUnderTest].History.CapturedAt()); diff != "" {
		t.Errorf("captured at (-want +got):\n%s", diff)
	}
}

// A capture whose write or commit failed must leave no trace: the status as it was and no
// event in the outbox, so the caller's retry captures it once rather than finding it captured
// with nothing announced — the money taken and the merchant's books never told.
func TestAChangeThatFailsToStoreLeavesNothingBehind(t *testing.T) {
	type args struct {
		failSave   error
		failCommit error
	}
	tests := []struct {
		name    string
		args    args
		want    row
		wantErr error
	}{
		{name: "the write fails", args: args{failSave: payment.ErrStorage}, want: row{Merchant: merchantOfIt, Status: "authorized", Outbox: []string{}}, wantErr: payment.ErrStorage},
		{name: "the commit's answer never arrives", args: args{failCommit: payment.ErrOutcomeUnknown}, want: row{Merchant: merchantOfIt, Status: "authorized", Outbox: []string{}}, wantErr: payment.ErrOutcomeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := dbWith("authorized")
			db.failSave = tt.args.failSave
			db.failCommit = tt.args.failCommit

			_, err := capture(db).Execute(t.Context(), ChangeCommand{MerchantID: merchantOfIt, PaymentID: paymentUnderTest})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			stored := db.rows[paymentUnderTest]
			stored.History = payment.History{}
			if diff := cmp.Diff(tt.want, stored, cmp.AllowUnexported(payment.History{})); diff != "" {
				t.Errorf("after the failed capture (-want +got):\n%s", diff)
			}

			db.failSave, db.failCommit = nil, nil
			if _, err := capture(db).Execute(t.Context(), ChangeCommand{MerchantID: merchantOfIt, PaymentID: paymentUnderTest}); err != nil {
				t.Fatalf("the retry: %v", err)
			}
			if diff := cmp.Diff([]string{"captured"}, db.rows[paymentUnderTest].Outbox); diff != "" {
				t.Errorf("the retry must announce the capture exactly once (-want +got):\n%s", diff)
			}
		})
	}
}

// statusNamed is the fake's own reading of a status it stored by name.
func statusNamed(name string) payment.Status {
	for _, status := range []payment.Status{payment.StatusAuthorized, payment.StatusCaptured, payment.StatusRefunded} {
		if status.String() == name {
			return status
		}
	}
	return payment.StatusUnknown
}
