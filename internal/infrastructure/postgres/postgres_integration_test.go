//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
)

const merchantUnderTest = "m-42"

func databaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Fatal("DATABASE_URL is empty: run these through `make test-integration`, which points them at the compose Postgres")
	}
	return url
}

func freshStorage(t *testing.T) (*postgres.Storage, *pgxpool.Pool) {
	t.Helper()
	url := databaseURL(t)

	storage, err := postgres.New(t.Context(), url)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(storage.Close)
	if err := storage.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatalf("open the inspecting pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(t.Context(), "TRUNCATE payments, outbox"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return storage, pool
}

func authorize(t *testing.T, minor int64) *payment.Payment {
	t.Helper()
	currency, err := payment.ParseCurrency("EUR")
	if err != nil {
		t.Fatalf("parse currency: %v", err)
	}
	amount, err := payment.NewMoney(minor, currency)
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	merchantID, err := payment.ParseMerchantID(merchantUnderTest)
	if err != nil {
		t.Fatalf("parse merchant: %v", err)
	}
	authorized, err := payment.Authorize(payment.NewID(), merchantID, amount, time.Now())
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	return authorized
}

func countPayments(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var stored int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM payments").Scan(&stored); err != nil {
		t.Fatalf("count payments: %v", err)
	}
	return stored
}

func countOutbox(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var stored int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM outbox").Scan(&stored); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return stored
}

func seedOutbox(t *testing.T, pool *pgxpool.Pool, records int) {
	t.Helper()
	for range records {
		_, err := pool.Exec(t.Context(),
			"INSERT INTO outbox (aggregate_id, event_type, payload, occurred_at) VALUES ($1, $2, $3, $4)",
			uuid.New().String(), "payment.authorized", []byte(`{"amount_minor":1}`), time.Now().UTC())
		if err != nil {
			t.Fatalf("seed outbox: %v", err)
		}
	}
}

func TestCreateOrGetStoresThePaymentAndTheEventItWillPublish(t *testing.T) {
	storage, pool := freshStorage(t)
	store := postgres.NewPaymentStore(storage)
	authorized := authorize(t, 1999)

	var (
		stored  payment.ID
		created bool
	)
	err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
		var err error
		stored, created, err = store.CreateOrGet(ctx, authorized, "key-1")
		return err
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !created {
		t.Error("the first authorisation reported a replay")
	}

	type row struct {
		Merchant string
		Minor    int64
		Currency string
		Status   string
	}
	var got row
	if err := pool.QueryRow(t.Context(),
		"SELECT merchant_id, amount_minor, currency, status FROM payments WHERE id = $1", stored.String(),
	).Scan(&got.Merchant, &got.Minor, &got.Currency, &got.Status); err != nil {
		t.Fatalf("read the payment back: %v", err)
	}
	want := row{Merchant: merchantUnderTest, Minor: 1999, Currency: "EUR", Status: "authorized"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("stored payment mismatch (-want +got):\n%s", diff)
	}

	var (
		eventType string
		payload   []byte
	)
	if err := pool.QueryRow(t.Context(),
		"SELECT event_type, payload FROM outbox WHERE aggregate_id = $1", stored.String(),
	).Scan(&eventType, &payload); err != nil {
		t.Fatalf("read the event back: %v", err)
	}
	if diff := cmp.Diff("payment.authorized", eventType); diff != "" {
		t.Errorf("event type mismatch (-want +got):\n%s", diff)
	}

	type published struct {
		PaymentID   string `json:"payment_id"`
		MerchantID  string `json:"merchant_id"`
		AmountMinor int64  `json:"amount_minor"`
		Currency    string `json:"currency"`
	}
	var decoded published
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode the payload a consumer will read: %v", err)
	}
	wantPayload := published{PaymentID: stored.String(), MerchantID: merchantUnderTest, AmountMinor: 1999, Currency: "EUR"}
	if diff := cmp.Diff(wantPayload, decoded); diff != "" {
		t.Errorf("payload mismatch (-want +got):\n%s", diff)
	}
}

// The outbox exists so that a consumer never learns about a payment the database does not
// hold. A rollback after the write must therefore take the event with it.
func TestCreateOrGetWhenTheTransactionFailsLeavesNeitherPaymentNorEvent(t *testing.T) {
	storage, pool := freshStorage(t)
	store := postgres.NewPaymentStore(storage)
	failure := errors.New("the rest of the use case failed")

	err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
		if _, _, err := store.CreateOrGet(ctx, authorize(t, 1999), "key-1"); err != nil {
			return err
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("err = %v, want %v", err, failure)
	}

	if diff := cmp.Diff(0, countPayments(t, pool)); diff != "" {
		t.Errorf("payments after a rollback (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(0, countOutbox(t, pool)); diff != "" {
		t.Errorf("events after a rollback (-want +got):\n%s", diff)
	}
}

func TestCreateOrGetReplayedWithTheSameKeyChargesTheMerchantOnce(t *testing.T) {
	storage, pool := freshStorage(t)
	store := postgres.NewPaymentStore(storage)

	authorizeOnce := func(amount int64) (payment.ID, bool) {
		t.Helper()
		var (
			stored  payment.ID
			created bool
		)
		err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
			var err error
			stored, created, err = store.CreateOrGet(ctx, authorize(t, amount), "key-1")
			return err
		})
		if err != nil {
			t.Fatalf("authorize: %v", err)
		}
		return stored, created
	}

	first, created := authorizeOnce(1999)
	if !created {
		t.Fatal("the first authorisation reported a replay")
	}
	replay, createdAgain := authorizeOnce(1999)
	if createdAgain {
		t.Error("the replay reported a stored payment; the merchant would be charged twice")
	}
	if diff := cmp.Diff(first.String(), replay.String()); diff != "" {
		t.Errorf("a replay produced a different payment (-first +replay):\n%s", diff)
	}
	if diff := cmp.Diff(1, countPayments(t, pool)); diff != "" {
		t.Errorf("payments stored (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(1, countOutbox(t, pool)); diff != "" {
		t.Errorf("events stored (-want +got):\n%s", diff)
	}
}

// Reusing a key for a different amount is a caller's bug, and the row already in the table
// is the authority on what that key bought.
func TestReusingAKeyForADifferentAmountIsRefusedAndChangesNothing(t *testing.T) {
	storage, pool := freshStorage(t)
	store := postgres.NewPaymentStore(storage)

	if err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
		_, _, err := store.CreateOrGet(ctx, authorize(t, 1999), "key-1")
		return err
	}); err != nil {
		t.Fatalf("first authorize: %v", err)
	}

	err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
		_, _, err := store.CreateOrGet(ctx, authorize(t, 500_000), "key-1")
		return err
	})
	if !errors.Is(err, payments.ErrIdempotencyKeyReused) {
		t.Fatalf("err = %v, want %v", err, payments.ErrIdempotencyKeyReused)
	}

	var minor int64
	if err := pool.QueryRow(t.Context(), "SELECT amount_minor FROM payments").Scan(&minor); err != nil {
		t.Fatalf("read the amount back: %v", err)
	}
	if diff := cmp.Diff(int64(1999), minor); diff != "" {
		t.Errorf("the stored amount changed (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(1, countOutbox(t, pool)); diff != "" {
		t.Errorf("events stored (-want +got):\n%s", diff)
	}
}

// A retrying client does not wait its turn: the same key arrives on several connections at
// once. Only the unique index decides, and only one caller may be told it created anything.
func TestConcurrentAuthorisationsWithTheSameKeyStoreOnePayment(t *testing.T) {
	storage, pool := freshStorage(t)
	store := postgres.NewPaymentStore(storage)

	const racers = 8
	type attempt struct {
		id      payment.ID
		created bool
	}
	attempts := make([]attempt, racers)

	group, ctx := errgroup.WithContext(t.Context())
	for racer := range racers {
		group.Go(func() error {
			return storage.WithinTx(ctx, func(ctx context.Context) error {
				id, created, err := store.CreateOrGet(ctx, authorize(t, 1999), "key-1")
				if err != nil {
					return err
				}
				attempts[racer] = attempt{id: id, created: created}
				return nil
			})
		})
	}
	if err := group.Wait(); err != nil {
		t.Fatalf("concurrent authorisations: %v", err)
	}

	creations := 0
	for _, made := range attempts {
		if made.created {
			creations++
		}
	}
	if diff := cmp.Diff(1, creations); diff != "" {
		t.Errorf("callers told they created a payment (-want +got):\n%s", diff)
	}
	for racer, made := range attempts {
		if diff := cmp.Diff(attempts[0].id.String(), made.id.String()); diff != "" {
			t.Errorf("racer %d got a different payment id (-first +got):\n%s", racer, diff)
		}
	}
	if diff := cmp.Diff(1, countPayments(t, pool)); diff != "" {
		t.Errorf("payments stored (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(1, countOutbox(t, pool)); diff != "" {
		t.Errorf("events stored (-want +got):\n%s", diff)
	}
}

// The constraint is the last line of defence: a bug in the domain must not be able to park
// a zero or negative authorisation in the table.
func TestSchemaRefusesAnAmountThatReservesNothing(t *testing.T) {
	_, pool := freshStorage(t)

	tests := []struct {
		name  string
		minor int64
	}{
		{name: "zero", minor: 0},
		{name: "negative", minor: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(),
				"INSERT INTO payments (id, merchant_id, amount_minor, currency, status, idempotency_key, authorized_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
				uuid.New().String(), merchantUnderTest, tt.minor, "EUR", "authorized", "key-"+tt.name, time.Now().UTC())
			if err == nil {
				t.Fatalf("the schema accepted an amount of %d", tt.minor)
			}
			if diff := cmp.Diff(0, countPayments(t, pool)); diff != "" {
				t.Errorf("payments stored (-want +got):\n%s", diff)
			}
		})
	}
}

// Two relays are the normal case during a rolling deploy. SKIP LOCKED is what stops the
// second one from waiting on the first — and from publishing the same event again.
func TestClaimHandsEachRecordToExactlyOneRelay(t *testing.T) {
	storage, pool := freshStorage(t)
	seedOutbox(t, pool, 4)
	store := postgres.NewOutboxStore(storage)

	var first, second []outbox.Record
	firstHolds := make(chan struct{})
	secondClaimed := make(chan struct{})

	// Without SKIP LOCKED the second relay would wait for the first to commit instead of
	// stepping over its rows. The deadline turns that into a failed test, not a hung suite.
	bounded, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	group, ctx := errgroup.WithContext(bounded)
	group.Go(func() error {
		return storage.WithinTx(ctx, func(ctx context.Context) error {
			claimed, err := store.Claim(ctx, 2)
			if err != nil {
				return err
			}
			first = claimed
			close(firstHolds)
			select {
			case <-secondClaimed:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	})
	group.Go(func() error {
		return storage.WithinTx(ctx, func(ctx context.Context) error {
			select {
			case <-firstHolds:
			case <-ctx.Done():
				return ctx.Err()
			}
			claimed, err := store.Claim(ctx, 2)
			if err != nil {
				return err
			}
			second = claimed
			close(secondClaimed)
			return nil
		})
	})
	if err := group.Wait(); err != nil {
		t.Fatalf("two relays claiming: %v", err)
	}

	if diff := cmp.Diff(2, len(first)); diff != "" {
		t.Fatalf("records the first relay claimed (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(2, len(second)); diff != "" {
		t.Fatalf("records the second relay claimed (-want +got):\n%s", diff)
	}
	held := map[int64]bool{}
	for _, record := range slices.Concat(first, second) {
		if held[record.ID] {
			t.Errorf("outbox record %d was handed to both relays; it would be published twice", record.ID)
		}
		held[record.ID] = true
	}
	if diff := cmp.Diff(4, len(held)); diff != "" {
		t.Errorf("distinct records claimed (-want +got):\n%s", diff)
	}
}

// The atomicity of the payment and its event is what makes the outbox trustworthy, so the
// store refuses the call that would quietly split them into two autocommits.
func TestCreateOrGetOutsideATransactionIsRefused(t *testing.T) {
	storage, pool := freshStorage(t)
	store := postgres.NewPaymentStore(storage)

	_, _, err := store.CreateOrGet(t.Context(), authorize(t, 1999), "key-1")
	if !errors.Is(err, postgres.ErrOutsideTransaction) {
		t.Fatalf("err = %v, want %v", err, postgres.ErrOutsideTransaction)
	}
	if diff := cmp.Diff(0, countPayments(t, pool)); diff != "" {
		t.Errorf("payments stored (-want +got):\n%s", diff)
	}
}

func TestClaimRefusesABatchItWouldHaveToAllocateFor(t *testing.T) {
	storage, _ := freshStorage(t)
	store := postgres.NewOutboxStore(storage)

	tests := []struct {
		name  string
		limit int
	}{
		{name: "nothing to claim", limit: 0},
		{name: "negative", limit: -1},
		{name: "above the batch ceiling", limit: 1001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
				_, err := store.Claim(ctx, tt.limit)
				return err
			})
			if !errors.Is(err, postgres.ErrClaimLimit) {
				t.Fatalf("err = %v, want %v", err, postgres.ErrClaimLimit)
			}
		})
	}
}

func TestClaimOutsideATransactionIsRefused(t *testing.T) {
	storage, pool := freshStorage(t)
	seedOutbox(t, pool, 1)
	store := postgres.NewOutboxStore(storage)

	_, err := store.Claim(t.Context(), 1)
	if !errors.Is(err, postgres.ErrOutsideTransaction) {
		t.Fatalf("err = %v, want %v", err, postgres.ErrOutsideTransaction)
	}
}

// A relay that published and died before it could record that will try again. The second
// attempt must change nothing, or the record would look fresh to the next sweep.
func TestMarkPublishedTwiceKeepsTheFirstPublicationTime(t *testing.T) {
	storage, pool := freshStorage(t)
	seedOutbox(t, pool, 1)
	store := postgres.NewOutboxStore(storage)

	var claimed []outbox.Record
	published := time.Now().UTC().Truncate(time.Millisecond)
	err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
		var err error
		claimed, err = store.Claim(ctx, 10)
		if err != nil {
			return err
		}
		return store.MarkPublished(ctx, []int64{claimed[0].ID}, published)
	})
	if err != nil {
		t.Fatalf("first publication: %v", err)
	}

	later := published.Add(time.Hour)
	if err := storage.WithinTx(t.Context(), func(ctx context.Context) error {
		return store.MarkPublished(ctx, []int64{claimed[0].ID}, later)
	}); err != nil {
		t.Fatalf("second publication: %v", err)
	}

	var stored time.Time
	if err := pool.QueryRow(t.Context(), "SELECT published_at FROM outbox WHERE id = $1", claimed[0].ID).Scan(&stored); err != nil {
		t.Fatalf("read published_at back: %v", err)
	}
	if !stored.Equal(published) {
		t.Errorf("published_at = %s, want the first publication at %s", stored, published)
	}

	var unpublished int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&unpublished); err != nil {
		t.Fatalf("count unpublished: %v", err)
	}
	if diff := cmp.Diff(0, unpublished); diff != "" {
		t.Errorf("records still waiting to be published (-want +got):\n%s", diff)
	}
}
