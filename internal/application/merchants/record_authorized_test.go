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
type fakeStore struct {
	claimed   map[string]bool
	totals    map[string]int64
	staged    []func()
	failAdd   error
	failClaim error
}

func newFakeStore() *fakeStore {
	return &fakeStore{claimed: make(map[string]bool), totals: make(map[string]int64)}
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

func (f *fakeStore) Add(_ context.Context, authorization merchant.Authorization, _ time.Time) error {
	if f.failAdd != nil {
		return f.failAdd
	}
	f.staged = append(f.staged, func() { f.totals[authorization.Merchant().String()] += authorization.Minor() })
	return nil
}

func harness(t *testing.T, store *fakeStore) *RecordAuthorized {
	t.Helper()
	at := time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
	return NewRecordAuthorized(store, store, store, func() time.Time { return at })
}

func authorized(eventID string, minor int64) AuthorizedEvent {
	return AuthorizedEvent{EventID: eventID, MerchantID: "m-42", AmountMinor: minor}
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
	if diff := cmp.Diff(map[string]int64{"m-42": 1999}, store.totals); diff != "" {
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
	if diff := cmp.Diff(map[string]int64{"m-42": 1999}, store.totals); diff != "" {
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

	if diff := cmp.Diff(map[string]int64{"m-42": 2000}, store.totals); diff != "" {
		t.Errorf("totals mismatch (-want +got):\n%s", diff)
	}
}

// The failure this design exists to survive: the claim was written, the total was not. If
// the claim outlived the rollback, the retry would see a handled event and the merchant
// would be short that payment for good — silently, with the offset already committed.
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
	if diff := cmp.Diff(map[string]int64{"m-42": 1999}, store.totals); diff != "" {
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
		mutate func(event *AuthorizedEvent)
	}
	tests := []struct {
		name    string
		args    args
		want    error
		wantErr error
	}{
		{
			name:    "no event id",
			args:    args{mutate: func(event *AuthorizedEvent) { event.EventID = "" }},
			want:    merchant.ErrEventIDRequired,
			wantErr: ErrUnprocessable,
		},
		{
			name:    "a merchant id longer than the payments domain can ever issue",
			args:    args{mutate: func(event *AuthorizedEvent) { event.MerchantID = strings.Repeat("m", 65) }},
			want:    merchant.ErrIDTooLong,
			wantErr: ErrUnprocessable,
		},
		{
			name:    "zero",
			args:    args{mutate: func(event *AuthorizedEvent) { event.AmountMinor = 0 }},
			want:    merchant.ErrAmountNotPositive,
			wantErr: ErrUnprocessable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			uc := harness(t, store)
			event := authorized("e-1", 1999)
			tt.args.mutate(&event)

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
