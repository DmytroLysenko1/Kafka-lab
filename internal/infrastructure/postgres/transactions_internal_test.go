package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

var errDriver = errors.New("dial tcp: connection refused")

// Each context answers its caller in its own words. For payments the two failures must stay
// apart: a storage failure means the payment was not kept and a retry is safe; an unknown
// outcome means it may have been kept, and only a retry with the same key finds out. Reporting
// an unknown commit as a plain failure would tell a client its payment did not happen when it
// may have. The consumer and the relay repeat their work either way, and say both as storage.
func TestATransactionFailureIsSaidInItsContextsWords(t *testing.T) {
	type args struct {
		transactions *Transactions
		err          error
	}
	type want struct {
		NotApplied bool
		Unknown    bool
	}
	payments := NewPaymentTransactions(nil)
	merchants := NewMerchantTransactions(nil)
	relay := NewOutboxTransactions(nil)
	tests := []struct {
		name string
		args args
		want want
	}{
		{name: "payments: the transaction never began", args: args{transactions: payments, err: errors.Join(ErrNotApplied, errDriver)}, want: want{NotApplied: true}},
		{name: "payments: the server rolled back at commit", args: args{transactions: payments, err: errors.Join(ErrNotApplied, &pgconn.PgError{Code: "40001"})}, want: want{NotApplied: true}},
		{name: "payments: the commit's answer never arrived", args: args{transactions: payments, err: errors.Join(ErrCommitUnknown, context.DeadlineExceeded)}, want: want{Unknown: true}},
		{name: "payments: the work inside failed, already in the payments' words", args: args{transactions: payments, err: payment.ErrNotTheSamePayment}},
		{name: "payments: nothing failed", args: args{transactions: payments}},
		{name: "merchants: the commit's answer never arrived", args: args{transactions: merchants, err: errors.Join(ErrCommitUnknown, context.DeadlineExceeded)}, want: want{NotApplied: true, Unknown: true}},
		{name: "merchants: the transaction never began", args: args{transactions: merchants, err: errors.Join(ErrNotApplied, errDriver)}, want: want{NotApplied: true, Unknown: true}},
		{name: "outbox: the commit's answer never arrived", args: args{transactions: relay, err: errors.Join(ErrCommitUnknown, context.DeadlineExceeded)}, want: want{NotApplied: true, Unknown: true}},
		{name: "outbox: the transaction never began", args: args{transactions: relay, err: errors.Join(ErrNotApplied, errDriver)}, want: want{NotApplied: true, Unknown: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translated := tt.args.transactions.inContextTerms(tt.args.err)

			got := want{
				NotApplied: errors.Is(translated, tt.args.transactions.notApplied),
				Unknown:    errors.Is(translated, tt.args.transactions.unknown),
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("translated %v (-want +got):\n%s", tt.args.err, diff)
			}
			if tt.args.err != nil && !errors.Is(translated, tt.args.err) {
				t.Errorf("the original error was dropped from %v", translated)
			}
			if tt.args.err == nil && translated != nil {
				t.Errorf("a transaction that succeeded came back as %v", translated)
			}
		})
	}
}

// The sentinels a context is told in are the ones its callers branch on: the HTTP layer maps
// payment.ErrOutcomeUnknown to "repeat with the same key", and nothing outside postgres may
// have to know ErrCommitUnknown to make that choice.
func TestEachContextIsToldInItsOwnSentinels(t *testing.T) {
	type args struct {
		transactions *Transactions
		err          error
	}
	tests := []struct {
		name string
		args args
		want error
	}{
		{name: "payments, not applied", args: args{transactions: NewPaymentTransactions(nil), err: ErrNotApplied}, want: payment.ErrStorage},
		{name: "payments, unknown", args: args{transactions: NewPaymentTransactions(nil), err: ErrCommitUnknown}, want: payment.ErrOutcomeUnknown},
		{name: "merchants, unknown", args: args{transactions: NewMerchantTransactions(nil), err: ErrCommitUnknown}, want: merchant.ErrStorage},
		{name: "outbox, unknown", args: args{transactions: NewOutboxTransactions(nil), err: ErrCommitUnknown}, want: outbox.ErrStorage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.args.transactions.inContextTerms(tt.args.err)
			if !errors.Is(got, tt.want) {
				t.Errorf("translated %v to %v, want it to be %v", tt.args.err, got, tt.want)
			}
		})
	}
}
