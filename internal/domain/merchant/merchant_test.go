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
	}
	type want struct {
		eventID  string
		merchant string
		minor    int64
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "an ordinary authorisation",
			args: args{eventID: "e-1", merchantID: "m-42", minor: 1999},
			want: want{eventID: "e-1", merchant: "m-42", minor: 1999},
		},
		{
			name: "the largest amount the total can take",
			args: args{eventID: "e-1", merchantID: "m-42", minor: 1 << 50},
			want: want{eventID: "e-1", merchant: "m-42", minor: 1 << 50},
		},
		{
			name:    "no event id: nothing to deduplicate by, so a redelivery would double the total",
			args:    args{eventID: "", merchantID: "m-42", minor: 1999},
			wantErr: merchant.ErrEventIDRequired,
		},
		{
			name:    "an event id a hostile producer could put a megabyte in",
			args:    args{eventID: strings.Repeat("e", 65), merchantID: "m-42", minor: 1999},
			wantErr: merchant.ErrEventIDTooLong,
		},
		{
			name:    "no merchant",
			args:    args{eventID: "e-1", merchantID: "", minor: 1999},
			wantErr: merchant.ErrIDRequired,
		},
		{
			name:    "a merchant id longer than the payments domain can ever issue",
			args:    args{eventID: "e-1", merchantID: strings.Repeat("m", 65), minor: 1999},
			wantErr: merchant.ErrIDTooLong,
		},
		{
			name:    "zero authorises nothing",
			args:    args{eventID: "e-1", merchantID: "m-42", minor: 0},
			wantErr: merchant.ErrAmountNotPositive,
		},
		{
			name:    "negative: a producer at another version could send one",
			args:    args{eventID: "e-1", merchantID: "m-42", minor: -1},
			wantErr: merchant.ErrAmountNotPositive,
		},
		{
			name:    "one past the bound the total is kept safe by",
			args:    args{eventID: "e-1", merchantID: "m-42", minor: 1<<50 + 1},
			wantErr: merchant.ErrAmountOutOfBalance,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := merchant.NewAuthorization(tt.args.eventID, tt.args.merchantID, tt.args.minor)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, want{eventID: got.EventID(), merchant: got.Merchant().String(), minor: got.Minor()}, cmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("authorisation (-want +got):\n%s", diff)
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
