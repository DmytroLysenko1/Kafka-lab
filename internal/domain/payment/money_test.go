package payment_test

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

func TestParseCurrencyAcceptsOnlyThreeUpperCaseLetters(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		wantErr error
	}{
		{name: "iso code", args: "EUR"},
		{name: "lower case, as a careless caller would send it", args: "eur", wantErr: payment.ErrCurrencyFormat},
		{name: "too short", args: "EU", wantErr: payment.ErrCurrencyFormat},
		{name: "too long", args: "EURO", wantErr: payment.ErrCurrencyFormat},
		{name: "digits", args: "E1R", wantErr: payment.ErrCurrencyFormat},
		{name: "empty", args: "", wantErr: payment.ErrCurrencyFormat},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := payment.ParseCurrency(tt.args)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if diff := cmp.Diff(tt.args, got.String()); diff != "" {
				t.Errorf("currency mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewMoneyRefusesWhatCannotBeAnAmount(t *testing.T) {
	eur, err := payment.ParseCurrency("EUR")
	if err != nil {
		t.Fatalf("parse currency: %v", err)
	}

	type args struct {
		minor    int64
		currency payment.Currency
	}

	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{name: "an ordinary amount", args: args{minor: 1999, currency: eur}},
		{name: "zero is an amount, even if it cannot be authorised", args: args{minor: 0, currency: eur}},
		{name: "the largest amount int64 holds, so overflow is visible if it ever appears", args: args{minor: 1<<63 - 1, currency: eur}},
		{name: "negative", args: args{minor: -1, currency: eur}, wantErr: payment.ErrAmountNegative},
		{name: "no currency", args: args{minor: 100}, wantErr: payment.ErrCurrencyFormat},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := payment.NewMoney(tt.args.minor, tt.args.currency)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if diff := cmp.Diff(tt.args.minor, got.Minor()); diff != "" {
				t.Errorf("minor units mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
