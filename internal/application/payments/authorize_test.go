package payments

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

var errStore = errors.New("store is down")

type storedPayment struct {
	Merchant string
	Minor    int64
	Currency string
	Status   string
	Events   int
}

// fakeDB is the store and the transaction manager at once, because that is what it is in
// production: writes made inside WithinTx land only when it commits, so a test can tell a
// rollback from a write that never happened.
type fakeDB struct {
	payments   map[string]storedPayment
	byKey      map[string]string
	staged     []func()
	failStore  error
	failCommit error
}

func newFakeDB() *fakeDB {
	return &fakeDB{payments: make(map[string]storedPayment), byKey: make(map[string]string)}
}

func (db *fakeDB) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	db.staged = nil
	if err := fn(ctx); err != nil {
		db.staged = nil
		return err
	}
	if db.failCommit != nil {
		db.staged = nil
		return db.failCommit
	}
	for _, apply := range db.staged {
		apply()
	}
	db.staged = nil
	return nil
}

func (db *fakeDB) CreateOrGet(_ context.Context, authorized *payment.Payment, idempotencyKey string) (payment.ID, bool, error) {
	if db.failStore != nil {
		return payment.ID{}, false, db.failStore
	}

	key := authorized.Merchant().String() + "|" + idempotencyKey
	if existing, taken := db.byKey[key]; taken {
		id, err := payment.ParseID(existing)
		return id, false, err
	}

	id := authorized.ID()
	row := storedPayment{
		Merchant: authorized.Merchant().String(),
		Minor:    authorized.Amount().Minor(),
		Currency: authorized.Amount().Currency().String(),
		Status:   authorized.Status().String(),
		Events:   len(authorized.PullEvents()),
	}
	db.staged = append(db.staged, func() {
		db.byKey[key] = id.String()
		db.payments[id.String()] = row
	})
	return id, true, nil
}

type ids struct {
	next []payment.ID
}

func (g *ids) take() payment.ID {
	id := g.next[0]
	if len(g.next) > 1 {
		g.next = g.next[1:]
	}
	return id
}

func harness(t *testing.T, db *fakeDB, generated ...payment.ID) *AuthorizePayment {
	t.Helper()
	at := time.Date(2026, time.September, 24, 12, 0, 0, 0, time.UTC)
	generator := &ids{next: generated}
	return NewAuthorizePayment(db, db, func() time.Time { return at }, generator.take)
}

func mustID(t *testing.T, value string) payment.ID {
	t.Helper()
	id, err := payment.ParseID(value)
	if err != nil {
		t.Fatalf("parse id %q: %v", value, err)
	}
	return id
}

func validCommand() AuthorizeCommand {
	return AuthorizeCommand{MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR", IdempotencyKey: "key-1"}
}

func TestAuthorizeStoresThePaymentAndItsEventTogether(t *testing.T) {
	db := newFakeDB()
	id := mustID(t, "11111111-1111-4111-8111-111111111111")
	uc := harness(t, db, id)

	got, err := uc.Execute(t.Context(), validCommand())
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	want := AuthorizeResult{PaymentID: id.String(), Stored: true}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
	wantRows := map[string]storedPayment{
		id.String(): {Merchant: "m-42", Minor: 1999, Currency: "EUR", Status: "authorized", Events: 1},
	}
	if diff := cmp.Diff(wantRows, db.payments); diff != "" {
		t.Errorf("stored payments mismatch (-want +got):\n%s", diff)
	}
}

func TestAuthorizeReplayedWithTheSameKeyChargesTheMerchantOnce(t *testing.T) {
	db := newFakeDB()
	first := mustID(t, "11111111-1111-4111-8111-111111111111")
	second := mustID(t, "22222222-2222-4222-8222-222222222222")
	uc := harness(t, db, first, second)

	initial, err := uc.Execute(t.Context(), validCommand())
	if err != nil {
		t.Fatalf("first authorize: %v", err)
	}
	replay, err := uc.Execute(t.Context(), validCommand())
	if err != nil {
		t.Fatalf("replayed authorize: %v", err)
	}

	if diff := cmp.Diff(initial.PaymentID, replay.PaymentID); diff != "" {
		t.Errorf("a replay produced a different payment (-first +replay):\n%s", diff)
	}
	if replay.Stored {
		t.Error("the replay reported a stored payment; the merchant would be charged twice")
	}
	if diff := cmp.Diff(1, len(db.payments)); diff != "" {
		t.Errorf("payments stored mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(1, db.payments[initial.PaymentID].Events); diff != "" {
		t.Errorf("events stored mismatch (-want +got):\n%s", diff)
	}
}

func TestAuthorizeWithADifferentKeyIsADifferentPayment(t *testing.T) {
	db := newFakeDB()
	first := mustID(t, "11111111-1111-4111-8111-111111111111")
	second := mustID(t, "22222222-2222-4222-8222-222222222222")
	uc := harness(t, db, first, second)

	if _, err := uc.Execute(t.Context(), validCommand()); err != nil {
		t.Fatalf("first authorize: %v", err)
	}
	other := validCommand()
	other.IdempotencyKey = "key-2"
	if _, err := uc.Execute(t.Context(), other); err != nil {
		t.Fatalf("second authorize: %v", err)
	}

	if diff := cmp.Diff(2, len(db.payments)); diff != "" {
		t.Errorf("payments stored mismatch (-want +got):\n%s", diff)
	}
}

func TestAuthorizeRefusesCommandsThatWouldStoreNonsense(t *testing.T) {
	type args struct {
		mutate func(cmd *AuthorizeCommand)
	}

	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name:    "no merchant: the payment would belong to nobody",
			args:    args{mutate: func(cmd *AuthorizeCommand) { cmd.MerchantID = "" }},
			wantErr: payment.ErrMerchantRequired,
		},
		{
			name:    "amount of zero reserves nothing and still emits an event",
			args:    args{mutate: func(cmd *AuthorizeCommand) { cmd.AmountMinor = 0 }},
			wantErr: payment.ErrAmountNotPositive,
		},
		{
			name:    "negative amount",
			args:    args{mutate: func(cmd *AuthorizeCommand) { cmd.AmountMinor = -1 }},
			wantErr: payment.ErrAmountNegative,
		},
		{
			name:    "currency that is not a currency",
			args:    args{mutate: func(cmd *AuthorizeCommand) { cmd.Currency = "euro" }},
			wantErr: payment.ErrCurrencyFormat,
		},
		{
			// Without a key a retry from the caller creates a second payment, which is the
			// failure this use case exists to prevent.
			name:    "no idempotency key",
			args:    args{mutate: func(cmd *AuthorizeCommand) { cmd.IdempotencyKey = "" }},
			wantErr: ErrIdempotencyKeyRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newFakeDB()
			uc := harness(t, db, mustID(t, "11111111-1111-4111-8111-111111111111"))
			cmd := validCommand()
			tt.args.mutate(&cmd)

			_, err := uc.Execute(t.Context(), cmd)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(0, len(db.payments)); diff != "" {
				t.Errorf("a refused command still stored something (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAuthorizeWhenTheCommitFailsStoresNeitherPaymentNorEvent(t *testing.T) {
	db := newFakeDB()
	db.failCommit = errStore
	uc := harness(t, db, mustID(t, "11111111-1111-4111-8111-111111111111"))

	got, err := uc.Execute(t.Context(), validCommand())
	if !errors.Is(err, errStore) {
		t.Fatalf("err = %v, want %v", err, errStore)
	}
	if diff := cmp.Diff(AuthorizeResult{}, got); diff != "" {
		t.Errorf("a failed commit still reported a payment (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(0, len(db.payments)); diff != "" {
		t.Errorf("payments stored after a failed commit (-want +got):\n%s", diff)
	}
}

func TestAuthorizeWhenTheStoreFailsReportsItAndStoresNothing(t *testing.T) {
	db := newFakeDB()
	db.failStore = errStore
	uc := harness(t, db, mustID(t, "11111111-1111-4111-8111-111111111111"))

	_, err := uc.Execute(t.Context(), validCommand())
	if !errors.Is(err, errStore) {
		t.Fatalf("err = %v, want %v", err, errStore)
	}
	if diff := cmp.Diff(0, len(db.payments)); diff != "" {
		t.Errorf("payments stored after a failed write (-want +got):\n%s", diff)
	}
}
