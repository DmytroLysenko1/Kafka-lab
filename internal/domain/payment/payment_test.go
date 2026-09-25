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

const merchantUnderTest = "m-42"

func merchant(t *testing.T) payment.MerchantID {
	t.Helper()
	id, err := payment.ParseMerchantID(merchantUnderTest)
	if err != nil {
		t.Fatalf("parse merchant %q: %v", merchantUnderTest, err)
	}
	return id
}

func TestAuthorizeKeepsTheAmountItWasGivenAndStampsTheEventWithTheSameTime(t *testing.T) {
	at := time.Date(2026, time.September, 24, 9, 30, 0, 0, time.FixedZone("Kyiv", 3*60*60))
	id := payment.NewID()

	authorized, err := payment.Authorize(id, merchant(t), eur(t, 1999), at)
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
	authorized, err := payment.Authorize(payment.NewID(), merchant(t), eur(t, 500), time.Now())
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

// A replay stands only if it asks for the very payment the key bought. 1999 EUR and 1999
// USD are the trap a check comparing minor units alone would fall into; a charge the caller
// did not ask for is what the refusal exists to prevent.
func TestAReplayStandsOnlyForTheSamePayment(t *testing.T) {
	type args struct {
		replay payment.Money
		other  string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{name: "the same amount for the same merchant", args: args{replay: eur(t, 1999), other: merchantUnderTest}},
		{name: "a different amount", args: args{replay: eur(t, 2000), other: merchantUnderTest}, wantErr: payment.ErrNotTheSamePayment},
		{name: "the same number in another currency", args: args{replay: usd(t, 1999), other: merchantUnderTest}, wantErr: payment.ErrNotTheSamePayment},
		{name: "the same amount for another merchant", args: args{replay: eur(t, 1999), other: "m-43"}, wantErr: payment.ErrNotTheSamePayment},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			earlier := payment.Reconstitute(payment.NewID(), merchant(t), eur(t, 1999), payment.StatusAuthorized, time.Now())
			other, err := payment.ParseMerchantID(tt.args.other)
			if err != nil {
				t.Fatalf("merchant: %v", err)
			}
			replay, err := payment.Authorize(payment.NewID(), other, tt.args.replay, time.Now())
			if err != nil {
				t.Fatalf("authorize: %v", err)
			}

			if err := replay.Replays(earlier); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// A payment rebuilt from storage to compare a replay against must not announce itself
// again: its event went out when it was first authorised.
func TestAReconstitutedPaymentHasNothingToPublish(t *testing.T) {
	at := time.Date(2026, time.September, 26, 9, 0, 0, 0, time.FixedZone("EEST", 3*60*60))
	id := payment.NewID()

	stored := payment.Reconstitute(id, merchant(t), eur(t, 1999), payment.StatusAuthorized, at)

	if diff := cmp.Diff(0, len(stored.PullEvents())); diff != "" {
		t.Errorf("events (-want +got):\n%s", diff)
	}
	type view struct {
		ID, Merchant, Status string
		Minor                int64
		At                   time.Time
	}
	want := view{ID: id.String(), Merchant: merchantUnderTest, Status: "authorized", Minor: 1999, At: at.UTC()}
	got := view{ID: stored.ID().String(), Merchant: stored.Merchant().String(), Status: stored.Status().String(), Minor: stored.Amount().Minor(), At: stored.AuthorizedAt()}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("reconstituted payment (-want +got):\n%s", diff)
	}
}

// A status read back from storage that no payment can be in is a corrupted row; reading it
// as "unknown" would let a comparison go ahead against it.
func TestOnlyAStatusAPaymentCanBeInIsReadBack(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name    string
		args    args
		want    payment.Status
		wantErr error
	}{
		{name: "authorized", args: args{value: "authorized"}, want: payment.StatusAuthorized},
		{name: "unknown", args: args{value: "unknown"}, want: payment.StatusUnknown, wantErr: payment.ErrStatusUnknown},
		{name: "empty", args: args{value: ""}, want: payment.StatusUnknown, wantErr: payment.ErrStatusUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := payment.ParseStatus(tt.args.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("status (-want +got):\n%s", diff)
			}
		})
	}
}

func usd(t *testing.T, minor int64) payment.Money {
	t.Helper()
	amount, err := payment.ParseMoney(minor, "USD")
	if err != nil {
		t.Fatalf("money: %v", err)
	}
	return amount
}

func TestAuthorizeRefusesAnAmountThatReservesNothing(t *testing.T) {
	_, err := payment.Authorize(payment.NewID(), merchant(t), eur(t, 0), time.Now())
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
