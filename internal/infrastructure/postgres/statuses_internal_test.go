package postgres

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

// A status read back that no payment can be in is a corrupted row — or one a newer version
// wrote; reading it as "unknown" would let a comparison go ahead against it. Every status a
// payment can be in reads back as itself.
func TestOnlyAStatusAPaymentCanBeInIsReadBack(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    payment.Status
		wantErr error
	}{
		{name: "authorized", args: "authorized", want: payment.StatusAuthorized},
		{name: "captured", args: "captured", want: payment.StatusCaptured},
		{name: "refunded", args: "refunded", want: payment.StatusRefunded},
		{name: "the zero status's own name", args: "unknown", want: payment.StatusUnknown, wantErr: errStatusUnknown},
		{name: "a status a newer version might write", args: "voided", want: payment.StatusUnknown, wantErr: errStatusUnknown},
		{name: "empty", args: "", want: payment.StatusUnknown, wantErr: errStatusUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStoredStatus(tt.args)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("status (-want +got):\n%s", diff)
			}
			if err != nil {
				return
			}
			written, err := storedStatus(got)
			if err != nil {
				t.Fatalf("store %s: %v", got, err)
			}
			if diff := cmp.Diff(tt.args, written); diff != "" {
				t.Errorf("a status must be written as it is read (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheUnknownStatusIsNeverWritten(t *testing.T) {
	if _, err := storedStatus(payment.StatusUnknown); !errors.Is(err, errStatusUnknown) {
		t.Fatalf("err = %v, want %v", err, errStatusUnknown)
	}
}
