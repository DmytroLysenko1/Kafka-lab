package merchants

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
)

var errStore = errors.New("the database is not answering")

// fakeStore is the inbox, the totals and the transaction manager at once, as Postgres is.
// Writes are staged and applied only on commit, so a test can see the difference between a
// claim that was rolled back and one that stuck — the difference between an event that will
// be retried and an event nobody will ever count.
// totalKey is what the projection keys a sum by: a merchant's EUR and JPY are two totals, and
// each keeps its authorised, captured and refunded sums apart.
type totalKey struct {
	merchant string
	currency string
	kind     string
}

type fakeStore struct {
	claimed   map[string]bool
	totals    map[totalKey]int64
	staged    []func()
	failAdd   error
	failClaim error
}

func newFakeStore() *fakeStore {
	return &fakeStore{claimed: make(map[string]bool), totals: make(map[totalKey]int64)}
}

func (f *fakeStore) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	f.staged = nil
	if err := fn(ctx); err != nil {
		f.staged = nil
		return err
	}
	for _, apply := range f.staged {
		apply()
	}
	f.staged = nil
	return nil
}

func (f *fakeStore) Claim(_ context.Context, eventID string, _ time.Time) (bool, error) {
	if f.failClaim != nil {
		return false, f.failClaim
	}
	if f.claimed[eventID] {
		return false, nil
	}
	f.staged = append(f.staged, func() { f.claimed[eventID] = true })
	return true, nil
}

func (f *fakeStore) Add(_ context.Context, movement merchant.Movement, _ time.Time) error {
	if f.failAdd != nil {
		return f.failAdd
	}
	key := totalKey{merchant: movement.Merchant().String(), currency: movement.Amount().Currency().String(), kind: movement.Kind().String()}
	f.staged = append(f.staged, func() { f.totals[key] += movement.Amount().Minor() })
	return nil
}

// counting is when the consumer counts, in every test here.
func counting() time.Time {
	return time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
}

func harness(t *testing.T, store *fakeStore) *RecordMovement {
	t.Helper()
	return NewRecordMovement(store, store, store, counting)
}

// authorized is an event authorised an hour before it is counted.
func authorized(eventID string, minor int64) *PaymentEvent {
	return &PaymentEvent{Kind: merchant.KindAuthorized, EventID: eventID, MerchantID: "m-42", AmountMinor: minor, Currency: "EUR", OccurredAt: counting().Add(-time.Hour)}
}

func TestRecordingAnEventAddsItToTheMerchantsTotal(t *testing.T) {
	store := newFakeStore()
	uc := harness(t, store)

	counted, err := uc.Execute(t.Context(), authorized("e-1", 1999))
	if err != nil {
		t.Fatalf("record: %v", err)
	}

	if !counted {
		t.Error("the first delivery was reported as a duplicate")
	}
	if diff := cmp.Diff(map[totalKey]int64{{merchant: "m-42", currency: "EUR", kind: "authorized"}: 1999}, store.totals); diff != "" {
		t.Errorf("totals mismatch (-want +got):\n%s", diff)
	}
}

// At-least-once delivery means the same event arrives again; the merchant must not be
// credited twice for one authorisation.
func TestTheSameEventDeliveredTwiceIsCountedOnce(t *testing.T) {
	store := newFakeStore()
	uc := harness(t, store)

	if _, err := uc.Execute(t.Context(), authorized("e-1", 1999)); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	counted, err := uc.Execute(t.Context(), authorized("e-1", 1999))
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}

	if counted {
		t.Error("the redelivery was counted; the merchant's total would double")
	}
	if diff := cmp.Diff(map[totalKey]int64{{merchant: "m-42", currency: "EUR", kind: "authorized"}: 1999}, store.totals); diff != "" {
		t.Errorf("totals mismatch (-want +got):\n%s", diff)
	}
}

func TestDifferentEventsBothCount(t *testing.T) {
	store := newFakeStore()
	uc := harness(t, store)

	if _, err := uc.Execute(t.Context(), authorized("e-1", 1999)); err != nil {
		t.Fatalf("first event: %v", err)
	}
	if _, err := uc.Execute(t.Context(), authorized("e-2", 1)); err != nil {
		t.Fatalf("second event: %v", err)
	}

	if diff := cmp.Diff(map[totalKey]int64{{merchant: "m-42", currency: "EUR", kind: "authorized"}: 2000}, store.totals); diff != "" {
		t.Errorf("totals mismatch (-want +got):\n%s", diff)
	}
}

// The failure this design exists to survive: the claim was written, the total was not. If
// the claim outlived the rollback, the retry would see a handled event and the merchant
// would be short that payment for good — silently, with the offset already committed.
// Two payments of 1999 to one merchant, in EUR and in JPY, are two totals of 1999 — never one
// total of 3998, which is what a projection keyed by merchant alone reported.
func TestAmountsInDifferentCurrenciesAreNeverAddedTogether(t *testing.T) {
	store := newFakeStore()
	uc := harness(t, store)
	euros := authorized("e-1", 1999)
	yen := authorized("e-2", 1999)
	yen.Currency = "JPY"

	for _, event := range []*PaymentEvent{euros, yen} {
		if _, err := uc.Execute(t.Context(), event); err != nil {
			t.Fatalf("record %s: %v", event.EventID, err)
		}
	}

	want := map[totalKey]int64{
		{merchant: "m-42", currency: "EUR", kind: "authorized"}: 1999,
		{merchant: "m-42", currency: "JPY", kind: "authorized"}: 1999,
	}
	if diff := cmp.Diff(want, store.totals, cmp.AllowUnexported(totalKey{})); diff != "" {
		t.Errorf("totals (-want +got):\n%s", diff)
	}
}

func TestWhenTheTotalFailsTheEventIsStillCountedOnTheRetry(t *testing.T) {
	store := newFakeStore()
	store.failAdd = errStore
	uc := harness(t, store)

	if _, err := uc.Execute(t.Context(), authorized("e-1", 1999)); !errors.Is(err, errStore) {
		t.Fatalf("err = %v, want %v", err, errStore)
	}
	if diff := cmp.Diff(0, len(store.claimed)); diff != "" {
		t.Errorf("the rolled back claim survived (-want +got):\n%s", diff)
	}

	store.failAdd = nil
	counted, err := uc.Execute(t.Context(), authorized("e-1", 1999))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !counted {
		t.Fatal("the retry treated the event as already handled; the payment is lost")
	}
	if diff := cmp.Diff(map[totalKey]int64{{merchant: "m-42", currency: "EUR", kind: "authorized"}: 1999}, store.totals); diff != "" {
		t.Errorf("totals after the retry (-want +got):\n%s", diff)
	}
}

func TestWhenTheInboxFailsNothingIsCounted(t *testing.T) {
	store := newFakeStore()
	store.failClaim = errStore
	uc := harness(t, store)

	if _, err := uc.Execute(t.Context(), authorized("e-1", 1999)); !errors.Is(err, errStore) {
		t.Fatalf("err = %v, want %v", err, errStore)
	}
	if diff := cmp.Diff(0, len(store.totals)); diff != "" {
		t.Errorf("totals written despite the failure (-want +got):\n%s", diff)
	}
}

// Which events are countable is the domain's rule and is tested there. What the use case
// owns is the consequence: a refused event is unprocessable — the consumer's cue to archive
// it rather than hold the partition — it still says why, and it moves no total.
func TestAnEventTheDomainRefusesIsUnprocessableAndCountsNothing(t *testing.T) {
	type args struct {
		mutate func(event *PaymentEvent)
	}
	tests := []struct {
		name    string
		args    args
		want    error
		wantErr error
	}{
		{
			name:    "no event id",
			args:    args{mutate: func(event *PaymentEvent) { event.EventID = "" }},
			want:    merchant.ErrEventIDRequired,
			wantErr: ErrUnprocessable,
		},
		{
			name:    "a merchant id longer than the payments domain can ever issue",
			args:    args{mutate: func(event *PaymentEvent) { event.MerchantID = strings.Repeat("m", 65) }},
			want:    merchant.ErrIDTooLong,
			wantErr: ErrUnprocessable,
		},
		{
			name:    "zero",
			args:    args{mutate: func(event *PaymentEvent) { event.AmountMinor = 0 }},
			want:    merchant.ErrAmountNotPositive,
			wantErr: ErrUnprocessable,
		},
		{
			name:    "no currency: the amount could be added to any total",
			args:    args{mutate: func(event *PaymentEvent) { event.Currency = "" }},
			want:    merchant.ErrCurrencyFormat,
			wantErr: ErrUnprocessable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			uc := harness(t, store)
			event := authorized("e-1", 1999)
			tt.args.mutate(event)

			counted, err := uc.Execute(t.Context(), event)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("err = %v, want it to still name %v", err, tt.want)
			}
			if counted {
				t.Error("a refused event was reported as counted")
			}
			if diff := cmp.Diff(0, len(store.totals)+len(store.claimed)); diff != "" {
				t.Errorf("a refused event still moved a total or claimed the inbox (-want +got):\n%s", diff)
			}
		})
	}
}

// The inbox is sure to remember an event for 39 days after it happened (retention). One older
// than that — a letter replayed, archived and replayed again — might already be forgotten,
// and counting it could count its payment twice; it is refused into the dead letter topic,
// where a person can decide, and the total is not touched.
func TestAnEventOlderThanTheInboxRemembersIsRefusedNotCounted(t *testing.T) {
	const day = 24 * time.Hour
	type args struct {
		age time.Duration
	}
	type want struct {
		Counted bool
		Totals  map[totalKey]int64
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{name: "an hour old", args: args{age: time.Hour}, want: want{Counted: true, Totals: map[totalKey]int64{{merchant: "m-42", currency: "EUR", kind: "authorized"}: 1999}}},
		{name: "a minute inside the window", args: args{age: 39*day - time.Minute}, want: want{Counted: true, Totals: map[totalKey]int64{{merchant: "m-42", currency: "EUR", kind: "authorized"}: 1999}}},
		{name: "exactly at the window's end", args: args{age: 39 * day}, want: want{Totals: map[totalKey]int64{}}, wantErr: ErrUnprocessable},
		{name: "a replay two months late", args: args{age: 60 * day}, want: want{Totals: map[totalKey]int64{}}, wantErr: ErrUnprocessable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			event := authorized("e-1", 1999)
			event.OccurredAt = counting().Add(-tt.args.age)

			counted, err := harness(t, store).Execute(t.Context(), event)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, want{Counted: counted, Totals: store.totals}, cmp.AllowUnexported(totalKey{})); diff != "" {
				t.Errorf("outcome (-want +got):\n%s", diff)
			}
		})
	}
}
