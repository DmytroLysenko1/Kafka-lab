package payment_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

func eur(t *testing.T, minor int64) payment.Money {
	t.Helper()
	currency, err := payment.ParseCurrency("EUR")
	if err != nil {
		t.Fatalf("parse currency: %v", err)
	}
	amount, err := payment.NewMoney(minor, currency)
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	return amount
}

func merchant(t *testing.T, value string) payment.MerchantID {
	t.Helper()
	id, err := payment.ParseMerchantID(value)
	if err != nil {
		t.Fatalf("parse merchant %q: %v", value, err)
	}
	return id
}

func TestAuthorizeKeepsTheAmountItWasGivenAndStampsTheEventWithTheSameTime(t *testing.T) {
	at := time.Date(2026, time.September, 24, 9, 30, 0, 0, time.FixedZone("Kyiv", 3*60*60))
	id := payment.NewID()

	authorized, err := payment.Authorize(id, merchant(t, "m-42"), eur(t, 1999), at)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	if diff := cmp.Diff(int64(1999), authorized.Amount().Minor()); diff != "" {
		t.Errorf("amount mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff("authorized", authorized.Status().String()); diff != "" {
		t.Errorf("status mismatch (-want +got):\n%s", diff)
	}
	if !authorized.AuthorizedAt().Equal(at) {
		t.Errorf("authorized at = %s, want the instant it was given, %s", authorized.AuthorizedAt(), at)
	}
	if location := authorized.AuthorizedAt().Location(); location != time.UTC {
		t.Errorf("authorized at is in %s; events cross time zones and must carry UTC", location)
	}

	events := authorized.PullEvents()
	if diff := cmp.Diff(1, len(events)); diff != "" {
		t.Fatalf("events mismatch (-want +got):\n%s", diff)
	}
	raised, isAuthorized := events[0].(*payment.Authorized)
	if !isAuthorized {
		t.Fatalf("event = %T, want *payment.Authorized", events[0])
	}
	if diff := cmp.Diff(id.String(), raised.PaymentID().String()); diff != "" {
		t.Errorf("event payment id mismatch (-want +got):\n%s", diff)
	}
	if !raised.OccurredAt().Equal(authorized.AuthorizedAt()) {
		t.Errorf("event time %s differs from the payment's %s", raised.OccurredAt(), authorized.AuthorizedAt())
	}
}

// The repository publishes what PullEvents hands it. If a second call handed the same event
// again, a retried Save would publish the authorisation twice.
func TestPullEventsHandsEachEventOverExactlyOnce(t *testing.T) {
	authorized, err := payment.Authorize(payment.NewID(), merchant(t, "m-42"), eur(t, 500), time.Now())
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	if diff := cmp.Diff(1, len(authorized.PullEvents())); diff != "" {
		t.Fatalf("first pull mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(0, len(authorized.PullEvents())); diff != "" {
		t.Errorf("second pull mismatch (-want +got):\n%s", diff)
	}
}

func TestAuthorizeRefusesAnAmountThatReservesNothing(t *testing.T) {
	_, err := payment.Authorize(payment.NewID(), merchant(t, "m-42"), eur(t, 0), time.Now())
	if !errors.Is(err, payment.ErrAmountNotPositive) {
		t.Errorf("err = %v, want %v", err, payment.ErrAmountNotPositive)
	}
}

func TestParseMerchantIDRefusesWhatCannotIdentifyAMerchant(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		wantErr error
	}{
		{name: "empty", args: "", wantErr: payment.ErrMerchantRequired},
		{name: "longer than the column that stores it", args: string(make([]byte, 65)), wantErr: payment.ErrMerchantTooLong},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := payment.ParseMerchantID(tt.args)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseIDRejectsWhatIsNotAUUID(t *testing.T) {
	if _, err := payment.ParseID("not-a-uuid"); !errors.Is(err, payment.ErrIDFormat) {
		t.Errorf("err = %v, want %v", err, payment.ErrIDFormat)
	}
}
