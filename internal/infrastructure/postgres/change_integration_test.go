//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
)

// stored authorises a payment through the real store, the way the API does, and returns its id.
func stored(t *testing.T, storage *postgres.Storage, idempotencyKey string) string {
	t.Helper()
	authorized := authorize(t, 1999)
	err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
		_, _, err := postgres.NewPaymentStore(storage).CreateOrGet(ctx, authorized, key(t, idempotencyKey))
		return err
	})
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	return authorized.ID().String()
}

type moves struct {
	capture *payments.CapturePayment
	refund  *payments.RefundPayment
}

func movesOn(storage *postgres.Storage) moves {
	store := postgres.NewPaymentStore(storage)
	transactions := postgres.NewPaymentTransactions(storage)
	return moves{
		capture: payments.NewCapturePayment(store, transactions, time.Now),
		refund:  payments.NewRefundPayment(store, transactions, time.Now),
	}
}

func command(paymentID string) payments.ChangeCommand {
	return payments.ChangeCommand{MerchantID: merchantUnderTest, PaymentID: paymentID}
}

func outboxTypes(t *testing.T, pool *pgxpool.Pool, paymentID string) []string {
	t.Helper()
	rows, err := pool.Query(t.Context(), "SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id", paymentID)
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	defer rows.Close()
	types := []string{}
	for rows.Next() {
		var eventType string
		if err := rows.Scan(&eventType); err != nil {
			t.Fatalf("scan: %v", err)
		}
		types = append(types, eventType)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	return types
}

// A payment's whole life on the real schema: each move stored with its time, and its events in
// the outbox in the order they happened, all keyed by the payment — the order the relay
// publishes them in and the partition they share.
func TestALifeOfCaptureAndRefundIsStoredWithItsEventsInOrder(t *testing.T) {
	storage, pool := freshStorage(t)
	paymentID := stored(t, storage, "life")
	on := movesOn(storage)

	if _, err := on.capture.Execute(t.Context(), command(paymentID)); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if _, err := on.refund.Execute(t.Context(), command(paymentID)); err != nil {
		t.Fatalf("refund: %v", err)
	}

	var (
		status         string
		captured, back bool
	)
	err := pool.QueryRow(t.Context(), "SELECT status, captured_at IS NOT NULL, refunded_at IS NOT NULL FROM payments WHERE id = $1", paymentID).
		Scan(&status, &captured, &back)
	if err != nil {
		t.Fatalf("read the payment: %v", err)
	}
	type row struct {
		Status             string
		Captured, Refunded bool
		Outbox             []string
	}
	want := row{Status: "refunded", Captured: true, Refunded: true, Outbox: []string{"payment.authorized", "payment.captured", "payment.refunded"}}
	if diff := cmp.Diff(want, row{Status: status, Captured: captured, Refunded: back, Outbox: outboxTypes(t, pool, paymentID)}); diff != "" {
		t.Errorf("the payment's life (-want +got):\n%s", diff)
	}
}

// Many callers capture one payment at the same moment — a client retrying on a timeout while
// its first attempt is still in flight. Every one of them is told it is captured, and the
// outbox holds one capture: the row lock makes them decide one after another, and each after
// the first finds the payment already captured.
func TestConcurrentCapturesOfOnePaymentAnnounceOneCapture(t *testing.T) {
	storage, pool := freshStorage(t)
	paymentID := stored(t, storage, "racing-captures")
	on := movesOn(storage)

	var racers errgroup.Group
	for range 8 {
		racers.Go(func() error {
			_, err := on.capture.Execute(t.Context(), command(paymentID))
			return err
		})
	}
	if err := racers.Wait(); err != nil {
		t.Fatalf("a racing capture failed: %v", err)
	}

	if diff := cmp.Diff([]string{"payment.authorized", "payment.captured"}, outboxTypes(t, pool, paymentID)); diff != "" {
		t.Errorf("outbox (-want +got):\n%s", diff)
	}
}

// A refund sent while its capture is still in flight must never refund what was not captured.
// Whichever takes the row first decides: a refund first is refused, a refund second follows
// the capture — and the outbox never holds a refund before, or without, its capture.
func TestARefundRacingItsCaptureNeverRefundsWhatWasNotCaptured(t *testing.T) {
	storage, pool := freshStorage(t)
	on := movesOn(storage)
	var refundsFirst, refundsAfter atomic.Int32

	for attempt := range 20 {
		paymentID := stored(t, storage, "race-"+string(rune('a'+attempt)))
		var racers errgroup.Group
		racers.Go(func() error {
			_, err := on.capture.Execute(t.Context(), command(paymentID))
			return err
		})
		racers.Go(func() error {
			_, err := on.refund.Execute(t.Context(), command(paymentID))
			switch {
			case errors.Is(err, payment.ErrInvalidTransition):
				refundsFirst.Add(1)
				return nil
			case err == nil:
				refundsAfter.Add(1)
			}
			return err
		})
		if err := racers.Wait(); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}

		got := outboxTypes(t, pool, paymentID)
		allowed := [][]string{
			{"payment.authorized", "payment.captured"},
			{"payment.authorized", "payment.captured", "payment.refunded"},
		}
		if !cmp.Equal(got, allowed[0]) && !cmp.Equal(got, allowed[1]) {
			t.Errorf("attempt %d: outbox %v is neither a capture nor a capture and its refund", attempt, got)
		}
	}
	t.Logf("refund refused as first %d times, applied after the capture %d times", refundsFirst.Load(), refundsAfter.Load())
}

func TestAnotherMerchantsPaymentIsNotFound(t *testing.T) {
	storage, pool := freshStorage(t)
	paymentID := stored(t, storage, "someone-elses")

	_, err := movesOn(storage).capture.Execute(t.Context(), payments.ChangeCommand{MerchantID: "m-7", PaymentID: paymentID})

	if !errors.Is(err, payment.ErrNotFound) {
		t.Fatalf("err = %v, want %v", err, payment.ErrNotFound)
	}
	if diff := cmp.Diff([]string{"payment.authorized"}, outboxTypes(t, pool, paymentID)); diff != "" {
		t.Errorf("another merchant's capture moved the payment (-want +got):\n%s", diff)
	}
}

// The schema is the last line behind the domain: a status the domain does not know, a capture
// without its time, a refund of a payment never captured, an over-long key — each is refused by
// Postgres itself, whatever the code that wrote it believed.
func TestTheSchemaRefusesAPaymentTheDomainWouldNot(t *testing.T) {
	storage, pool := freshStorage(t)
	paymentID := stored(t, storage, "schema")
	tests := []struct {
		name string
		args string
	}{
		{name: "a status no payment can be in", args: "UPDATE payments SET status = 'voided' WHERE id = $1"},
		{name: "captured without a capture time", args: "UPDATE payments SET status = 'captured' WHERE id = $1"},
		{name: "refunded without ever being captured", args: "UPDATE payments SET status = 'refunded', refunded_at = now() WHERE id = $1"},
		{name: "a capture time on an authorisation", args: "UPDATE payments SET captured_at = now() WHERE id = $1"},
		{name: "an idempotency key past 255 bytes", args: "UPDATE payments SET idempotency_key = repeat('k', 256) WHERE id = $1"},
		{name: "a merchant id past 64 bytes", args: "UPDATE payments SET merchant_id = repeat('m', 65) WHERE id = $1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), tt.args, paymentID)
			pgErr, isPgErr := errors.AsType[*pgconn.PgError](err)
			if !isPgErr || pgErr.Code != "23514" {
				t.Fatalf("err = %v, want a check violation (23514)", err)
			}
		})
	}
}

// Each kind of movement is added to its own column, by the database: a capture counted into the
// authorised sum would report a merchant paid twice.
func TestEachKindOfMovementIsAddedToItsOwnSum(t *testing.T) {
	storage, pool := freshStorage(t)
	if _, err := pool.Exec(t.Context(), "TRUNCATE merchant_currency_totals"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	totals := postgres.NewTotalsStore(storage)
	add := func(kind merchant.Kind, eventID string, minor int64) {
		t.Helper()
		amount, err := merchant.NewAmount(minor, "EUR")
		if err != nil {
			t.Fatalf("amount: %v", err)
		}
		movement, err := merchant.NewMovement(kind, eventID, merchantUnderTest, amount)
		if err != nil {
			t.Fatalf("movement: %v", err)
		}
		if err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
			return totals.Add(ctx, movement, time.Now())
		}); err != nil {
			t.Fatalf("add %s: %v", kind, err)
		}
	}

	add(merchant.KindRefunded, "e-3", 500)
	add(merchant.KindAuthorized, "e-1", 1999)
	add(merchant.KindCaptured, "e-2", 1999)
	add(merchant.KindAuthorized, "e-4", 1)

	merchantID, err := merchant.ParseID(merchantUnderTest)
	if err != nil {
		t.Fatalf("merchant: %v", err)
	}
	read, err := totals.Totals(t.Context(), merchantID)
	if err != nil {
		t.Fatalf("read totals: %v", err)
	}
	type sums struct {
		Currency                       string
		Authorized, Captured, Refunded int64
	}
	got := make([]sums, 0, len(read))
	for _, total := range read {
		got = append(got, sums{Currency: total.Currency().String(), Authorized: total.Authorized(), Captured: total.Captured(), Refunded: total.Refunded()})
	}
	if diff := cmp.Diff([]sums{{Currency: "EUR", Authorized: 2000, Captured: 1999, Refunded: 500}}, got); diff != "" {
		t.Errorf("totals (-want +got):\n%s", diff)
	}
}

// A transaction opened inside another joins it: what the inner one wrote is part of the outer
// one and goes when the outer one rolls back. A nested call that committed on its own would
// keep a payment whose outer work — and whatever else it promised — never happened.
func TestANestedTransactionJoinsTheOneAroundIt(t *testing.T) {
	storage, pool := freshStorage(t)
	errOuterFailed := errors.New("the outer work failed after the inner one wrote")

	err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
		if err := storage.WithinTx(ctx, func(ctx context.Context) error {
			_, _, err := postgres.NewPaymentStore(storage).CreateOrGet(ctx, authorize(t, 1999), key(t, "nested"))
			return err
		}); err != nil {
			return err
		}
		return errOuterFailed
	})

	if !errors.Is(err, errOuterFailed) {
		t.Fatalf("err = %v, want %v", err, errOuterFailed)
	}
	if diff := cmp.Diff(0, countPayments(t, pool)+countOutbox(t, pool)); diff != "" {
		t.Errorf("the inner write outlived the outer rollback (-want +got):\n%s", diff)
	}
}
