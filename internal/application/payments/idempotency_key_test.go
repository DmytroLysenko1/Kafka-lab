package payments_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
)

// A key the unique index cannot hold must be refused as the caller's mistake: refused by
// Postgres it would read as an outage, and the same request would fail on every retry.
func TestAnIdempotencyKeyIsOnlyOneTheStoreCanHold(t *testing.T) {
	type args struct {
		raw string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr error
	}{
		{name: "an ordinary key", args: args{raw: "order-42-attempt"}, want: "order-42-attempt"},
		{name: "exactly at the limit", args: args{raw: strings.Repeat("k", payments.MaxIdempotencyKeyLength)}, want: strings.Repeat("k", payments.MaxIdempotencyKeyLength)},
		{name: "multi-byte text counted in bytes", args: args{raw: "ключ"}, want: "ключ"},
		{name: "no key at all", args: args{raw: ""}, wantErr: payments.ErrIdempotencyKeyRequired},
		{name: "one byte past the limit", args: args{raw: strings.Repeat("k", payments.MaxIdempotencyKeyLength+1)}, wantErr: payments.ErrIdempotencyKeyTooLong},
		{name: "bytes that are not UTF-8", args: args{raw: "key-\xff"}, wantErr: payments.ErrIdempotencyKeyInvalid},
		{name: "a NUL byte Postgres refuses in text", args: args{raw: "key-\x00"}, wantErr: payments.ErrIdempotencyKeyInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := payments.ParseIdempotencyKey(tt.args.raw)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got.String()); diff != "" {
				t.Errorf("key mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantErr != nil, got.IsZero()); diff != "" {
				t.Errorf("a refused key must be the zero key (-want +got):\n%s", diff)
			}
		})
	}
}
