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
	byKey      map[string]*payment.Payment
	staged     []func()
	failStore  error
	failCommit error
}

func newFakeDB() *fakeDB {
	return &fakeDB{payments: make(map[string]storedPayment), byKey: make(map[string]*payment.Payment)}
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

// CreateOrGet returns what the key already bought and decides nothing about it, as the real
// store does: whether a replay may stand is the payment's call, and a fake that made it
// would let the use case pass a test without ever asking.
func (db *fakeDB) CreateOrGet(_ context.Context, authorized *payment.Payment, idempotencyKey string) (*payment.Payment, bool, error) {
	if db.failStore != nil {
		return nil, false, db.failStore
	}

	key := authorized.Merchant().String() + "|" + idempotencyKey
	if earlier, taken := db.byKey[key]; taken {
		return earlier, false, nil
	}

	id := authorized.ID().String()
	row := storedPayment{
		Merchant: authorized.Merchant().String(),
		Minor:    authorized.Amount().Minor(),
		Currency: authorized.Amount().Currency().String(),
		Status:   authorized.Status().String(),
		Events:   len(authorized.PullEvents()),
	}
	stored := payment.Reconstitute(authorized.ID(), authorized.Merchant(), authorized.Amount(), authorized.Status(), authorized.AuthorizedAt())
	db.staged = append(db.staged, func() {
		db.byKey[key] = stored
		db.payments[id] = row
	})
	return authorized, true, nil
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

// A caller who reuses a key for a different amount is not replaying; it is a bug on their
// side. Handing back the earlier payment would tell them an amount they never authorised
// went through.
func TestAuthorizeReusingAKeyForADifferentAmountIsRefused(t *testing.T) {
	db := newFakeDB()
	uc := harness(t, db,
		mustID(t, "11111111-1111-4111-8111-111111111111"),
		mustID(t, "22222222-2222-4222-8222-222222222222"),
	)

	first, err := uc.Execute(t.Context(), validCommand())
	if err != nil {
		t.Fatalf("first authorize: %v", err)
	}

	larger := validCommand()
	larger.AmountMinor = 500_000
	got, err := uc.Execute(t.Context(), larger)
	if !errors.Is(err, ErrIdempotencyKeyReused) {
		t.Fatalf("err = %v, want %v", err, ErrIdempotencyKeyReused)
	}
	if !errors.Is(err, payment.ErrNotTheSamePayment) {
		t.Errorf("err = %v, want the payment's own reason, %v, inside it", err, payment.ErrNotTheSamePayment)
	}
	if diff := cmp.Diff(AuthorizeResult{}, got); diff != "" {
		t.Errorf("a refused reuse still reported a payment (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(int64(1999), db.payments[first.PaymentID].Minor); diff != "" {
		t.Errorf("the stored amount changed (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(1, len(db.payments)); diff != "" {
		t.Errorf("payments stored (-want +got):\n%s", diff)
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
