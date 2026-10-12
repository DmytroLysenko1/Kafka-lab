package merchants

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
)

func event(kind merchant.Kind, eventID string) *PaymentEvent {
	return &PaymentEvent{Kind: kind, EventID: eventID, MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR", OccurredAt: counting().Add(-time.Hour)}
}

// Each kind of movement goes into its own sum: a capture is money taken, not more money
// authorised, and adding it to the authorised sum would report a merchant paid twice.
func TestEachKindOfMovementIsCountedInItsOwnSum(t *testing.T) {
	store := newFakeStore()
	uc := harness(t, store)

	for _, movement := range []*PaymentEvent{
		event(merchant.KindAuthorized, "e-1"),
		event(merchant.KindCaptured, "e-2"),
		event(merchant.KindRefunded, "e-3"),
	} {
		if _, err := uc.Execute(t.Context(), movement); err != nil {
			t.Fatalf("record %s: %v", movement.Kind, err)
		}
	}

	want := map[totalKey]int64{
		{merchant: "m-42", currency: "EUR", kind: "authorized"}: 1999,
		{merchant: "m-42", currency: "EUR", kind: "captured"}:   1999,
		{merchant: "m-42", currency: "EUR", kind: "refunded"}:   1999,
	}
	if diff := cmp.Diff(want, store.totals, cmp.AllowUnexported(totalKey{})); diff != "" {
		t.Errorf("totals (-want +got):\n%s", diff)
	}
}

// The events of one payment arrive in order on the main topic, but a contended capture moved
// to a retry tier comes back after the refund behind it. The projection is a set of sums, so
// whatever order the consumer meets them in — and however many times — it ends on the same
// totals, and once every event is in, refunded ≤ captured ≤ authorized holds.
func TestTheTotalsEndTheSameWhateverOrderTheEventsArriveIn(t *testing.T) {
	authorize := event(merchant.KindAuthorized, "e-1")
	capture := event(merchant.KindCaptured, "e-2")
	refund := event(merchant.KindRefunded, "e-3")
	tests := []struct {
		name string
		args []*PaymentEvent
	}{
		{name: "in the order they happened", args: []*PaymentEvent{authorize, capture, refund}},
		{name: "the capture back from a retry tier after the refund", args: []*PaymentEvent{authorize, refund, capture}},
		{name: "the refund first of all", args: []*PaymentEvent{refund, authorize, capture}},
		{name: "every event delivered twice", args: []*PaymentEvent{authorize, refund, authorize, capture, refund, capture}},
	}
	want := map[totalKey]int64{
		{merchant: "m-42", currency: "EUR", kind: "authorized"}: 1999,
		{merchant: "m-42", currency: "EUR", kind: "captured"}:   1999,
		{merchant: "m-42", currency: "EUR", kind: "refunded"}:   1999,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			uc := harness(t, store)

			for _, movement := range tt.args {
				if _, err := uc.Execute(t.Context(), movement); err != nil {
					t.Fatalf("record %s %s: %v", movement.Kind, movement.EventID, err)
				}
			}

			if diff := cmp.Diff(want, store.totals, cmp.AllowUnexported(totalKey{})); diff != "" {
				t.Errorf("totals (-want +got):\n%s", diff)
			}
			sums := func(kind string) int64 { return store.totals[totalKey{merchant: "m-42", currency: "EUR", kind: kind}] }
			if sums("refunded") > sums("captured") || sums("captured") > sums("authorized") {
				t.Errorf("once every event is in, refunded %d <= captured %d <= authorized %d must hold",
					sums("refunded"), sums("captured"), sums("authorized"))
			}
		})
	}
}

// A kind the decoder could not name is a record nobody can count, and it is refused into the
// dead letter topic rather than added to a sum it might not belong to.
func TestAnEventOfNoKnownKindIsUnprocessable(t *testing.T) {
	store := newFakeStore()

	counted, err := harness(t, store).Execute(t.Context(), event(merchant.KindUnknown, "e-1"))

	if !errors.Is(err, ErrUnprocessable) || !errors.Is(err, merchant.ErrKindUnknown) {
		t.Fatalf("err = %v, want %v naming %v", err, ErrUnprocessable, merchant.ErrKindUnknown)
	}
	if counted || len(store.totals)+len(store.claimed) != 0 {
		t.Errorf("a kindless event moved the projection: counted %t, totals %v, claimed %v", counted, store.totals, store.claimed)
	}
}
