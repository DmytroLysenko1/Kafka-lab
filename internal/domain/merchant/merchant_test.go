package merchant_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
)

// Every value here arrives off a topic from a producer at any version. Each refusal names
// what would go wrong if the value were counted instead.
func TestAnAuthorizationThatCannotBeCountedSafelyCannotBeMade(t *testing.T) {
	type args struct {
		eventID    string
		merchantID string
		minor      int64
		currency   string
	}
	type want struct {
		eventID  string
		merchant string
		minor    int64
		currency string
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "an ordinary authorisation",
			args: args{eventID: "e-1", merchantID: "m-42", minor: 1999, currency: "EUR"},
			want: want{eventID: "e-1", merchant: "m-42", minor: 1999, currency: "EUR"},
		},
		{
			name: "the largest amount the total can take",
			args: args{eventID: "e-1", merchantID: "m-42", minor: 1 << 50, currency: "JPY"},
			want: want{eventID: "e-1", merchant: "m-42", minor: 1 << 50, currency: "JPY"},
		},
		{
			name:    "no event id: nothing to deduplicate by, so a redelivery would double the total",
			args:    args{eventID: "", merchantID: "m-42", minor: 1999, currency: "EUR"},
			wantErr: merchant.ErrEventIDRequired,
		},
		{
			name:    "an event id a hostile producer could put a megabyte in",
			args:    args{eventID: strings.Repeat("e", 65), merchantID: "m-42", minor: 1999, currency: "EUR"},
			wantErr: merchant.ErrEventIDTooLong,
		},
		{
			name:    "an event id with a NUL byte, which the inbox key cannot hold",
			args:    args{eventID: "e-\x00", merchantID: "m-42", minor: 1999, currency: "EUR"},
			wantErr: merchant.ErrEventIDInvalid,
		},
		{
			name:    "a merchant id that is not UTF-8, which the total's key cannot hold",
			args:    args{eventID: "e-1", merchantID: "m-\xff", minor: 1999, currency: "EUR"},
			wantErr: merchant.ErrIDInvalid,
		},
		{
			name:    "no merchant",
			args:    args{eventID: "e-1", merchantID: "", minor: 1999, currency: "EUR"},
			wantErr: merchant.ErrIDRequired,
		},
		{
			name:    "a merchant id longer than the payments domain can ever issue",
			args:    args{eventID: "e-1", merchantID: strings.Repeat("m", 65), minor: 1999, currency: "EUR"},
			wantErr: merchant.ErrIDTooLong,
		},
		{
			name:    "zero authorises nothing",
			args:    args{eventID: "e-1", merchantID: "m-42", minor: 0, currency: "EUR"},
			wantErr: merchant.ErrAmountNotPositive,
		},
		{
			name:    "negative: a producer at another version could send one",
			args:    args{eventID: "e-1", merchantID: "m-42", minor: -1, currency: "EUR"},
			wantErr: merchant.ErrAmountNotPositive,
		},
		{
			name:    "one past the bound the total is kept safe by",
			args:    args{eventID: "e-1", merchantID: "m-42", minor: 1<<50 + 1, currency: "EUR"},
			wantErr: merchant.ErrAmountTooLarge,
		},
		{
			name:    "no currency: the amount could be added to any total",
			args:    args{eventID: "e-1", merchantID: "m-42", minor: 1999, currency: ""},
			wantErr: merchant.ErrCurrencyFormat,
		},
		{
			name:    "a currency in lower case is not a code",
			args:    args{eventID: "e-1", merchantID: "m-42", minor: 1999, currency: "eur"},
			wantErr: merchant.ErrCurrencyFormat,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := authorization(tt.args.eventID, tt.args.merchantID, tt.args.minor, tt.args.currency)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			seen := want{
				eventID:  got.EventID(),
				merchant: got.Merchant().String(),
				minor:    got.Amount().Minor(),
				currency: got.Amount().Currency().String(),
			}
			if diff := cmp.Diff(tt.want, seen, cmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("authorisation (-want +got):\n%s", diff)
			}
		})
	}
}

// authorization builds the value the way the use case does: the amount first, then the
// movement around it; a refusal from either is the refusal.
func authorization(eventID, merchantID string, minor int64, currency string) (merchant.Movement, error) {
	amount, err := merchant.NewAmount(minor, currency)
	if err != nil {
		return merchant.Movement{}, err
	}
	return merchant.NewMovement(merchant.KindAuthorized, eventID, merchantID, amount)
}

// A movement is counted in the sum of its kind, so a movement of no kind has nowhere to go —
// and a kind past the last one is a value no decoder made.
func TestAMovementIsOfAKindATotalCounts(t *testing.T) {
	tests := []struct {
		name    string
		args    merchant.Kind
		want    string
		wantErr error
	}{
		{name: "authorized", args: merchant.KindAuthorized, want: "authorized"},
		{name: "captured", args: merchant.KindCaptured, want: "captured"},
		{name: "refunded", args: merchant.KindRefunded, want: "refunded"},
		{name: "no kind", args: merchant.KindUnknown, want: "unknown", wantErr: merchant.ErrKindUnknown},
		{name: "a kind past the last", args: merchant.KindRefunded + 1, want: "unknown", wantErr: merchant.ErrKindUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			amount, err := merchant.NewAmount(1999, "EUR")
			if err != nil {
				t.Fatalf("amount: %v", err)
			}
			got, err := merchant.NewMovement(tt.args, "e-1", "m-42", amount)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, tt.args.String()); diff != "" {
				t.Errorf("kind name (-want +got):\n%s", diff)
			}
			if err == nil && got.Kind() != tt.args {
				t.Errorf("movement kind = %s, want %s", got.Kind(), tt.args)
			}
		})
	}
}

// A total comes back from the projection; a row that no total could be is refused rather than
// shown to a merchant. The three sums are not held to refunded <= captured <= authorized: the
// projection passes through other orders while the retry tiers deliver.
func TestATotalReadBackIsCheckedLikeOneWritten(t *testing.T) {
	type args struct {
		currency                       string
		authorized, captured, refunded int64
	}
	type want struct {
		Currency                       string
		Authorized, Captured, Refunded int64
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{name: "a total", args: args{currency: "EUR", authorized: 240000, captured: 200000, refunded: 1999}, want: want{Currency: "EUR", Authorized: 240000, Captured: 200000, Refunded: 1999}},
		{name: "nothing yet", args: args{currency: "JPY"}, want: want{Currency: "JPY"}},
		{name: "a refund counted before its capture", args: args{currency: "EUR", authorized: 1999, refunded: 1999}, want: want{Currency: "EUR", Authorized: 1999, Refunded: 1999}},
		{name: "a negative authorised sum", args: args{currency: "EUR", authorized: -1}, wantErr: merchant.ErrTotalNegative},
		{name: "a negative captured sum", args: args{currency: "EUR", captured: -1}, wantErr: merchant.ErrTotalNegative},
		{name: "a negative refunded sum", args: args{currency: "EUR", refunded: -1}, wantErr: merchant.ErrTotalNegative},
		{name: "no currency", args: args{currency: "", authorized: 1}, wantErr: merchant.ErrCurrencyFormat},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := merchant.NewTotal(tt.args.currency, tt.args.authorized, tt.args.captured, tt.args.refunded)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			seen := want{Currency: got.Currency().String(), Authorized: got.Authorized(), Captured: got.Captured(), Refunded: got.Refunded()}
			if diff := cmp.Diff(tt.want, seen); diff != "" {
				t.Errorf("total (-want +got):\n%s", diff)
			}
		})
	}
}

// The read path asks with an id from a URL, which is no more trusted than a header.
func TestAMerchantIDIsBoundedTheWayTheProjectionKeysIt(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr error
	}{
		{name: "an id", args: args{value: "m-42"}, want: "m-42"},
		{name: "exactly the longest", args: args{value: strings.Repeat("m", 64)}, want: strings.Repeat("m", 64)},
		{name: "empty", args: args{value: ""}, wantErr: merchant.ErrIDRequired},
		{name: "one too long", args: args{value: strings.Repeat("m", 65)}, wantErr: merchant.ErrIDTooLong},
		{name: "a NUL byte", args: args{value: "m-\x00"}, wantErr: merchant.ErrIDInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := merchant.ParseID(tt.args.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got.String()); diff != "" {
				t.Errorf("id (-want +got):\n%s", diff)
			}
		})
	}
}
