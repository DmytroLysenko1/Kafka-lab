package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
)

// payments is who is who in the run, by event id: the inbox is keyed by it, and it is what
// the report splits the timings by.
type payments struct {
	all []string
	hot map[string]bool
}

// produce writes every payment before any consumer starts, so each cell meets the same
// backlog in the same order and the timings measure the consumer, not the producer.
func produce(ctx context.Context, s *settings) (payments, error) {
	publisher, err := kafka.NewPublisher(ctx, brokers(), s.topic("main"), os.Getenv("SCHEMA_REGISTRY_URL"))
	if err != nil {
		return payments{}, err
	}
	defer publisher.Close()

	produced := payments{all: make([]string, 0, s.payments), hot: make(map[string]bool)}
	records := make([]outbox.Record, 0, s.payments)
	for i := range s.payments {
		record, err := paymentRecord(s, i)
		if err != nil {
			return payments{}, err
		}
		records = append(records, record)

		eventID := strconv.FormatInt(record.ID, 10)
		produced.all = append(produced.all, eventID)
		if s.isHot(i) {
			produced.hot[eventID] = true
		}
	}
	if err := publisher.Publish(ctx, records); err != nil {
		return payments{}, fmt.Errorf("produce %d payments: %w", s.payments, err)
	}
	return produced, nil
}

func paymentRecord(s *settings, i int) (outbox.Record, error) {
	paymentID := fmt.Sprintf("exp18-%s-%d", s.cell, i)
	occurred := time.Now().UTC()
	payload, err := json.Marshal(outbox.AuthorizedPayload{
		PaymentID:   paymentID,
		MerchantID:  s.merchantFor(i),
		AmountMinor: 1,
		Currency:    "EUR",
		OccurredAt:  occurred,
	})
	if err != nil {
		return outbox.Record{}, err
	}
	return outbox.Record{
		ID:          s.firstEventID + int64(i),
		AggregateID: paymentID,
		EventType:   outbox.EventTypeAuthorized,
		Payload:     payload,
		OccurredAt:  occurred,
	}, nil
}

// lock is the other transaction: a report or a reconciliation that took the merchant's row
// and has not let go. release may be called by the cell's operator and by the deferred
// cleanup alike; the row is released once.
type lock struct {
	release func() error
}

func lockMerchant(ctx context.Context, pool *pgxpool.Pool, merchant string) (lock, error) {
	if _, err := pool.Exec(ctx,
		"INSERT INTO merchant_totals (merchant_id, authorized_minor, updated_at) VALUES ($1, 0, now())", merchant); err != nil {
		return lock{}, fmt.Errorf("create the row of %s: %w", merchant, err)
	}
	holder, err := pool.Begin(ctx)
	if err != nil {
		return lock{}, fmt.Errorf("begin the holding transaction: %w", err)
	}
	if _, err := holder.Exec(ctx, "SELECT 1 FROM merchant_totals WHERE merchant_id = $1 FOR UPDATE", merchant); err != nil {
		return lock{}, fmt.Errorf("lock the row of %s: %w", merchant, err)
	}
	return lock{release: sync.OnceValue(func() error {
		return holder.Rollback(context.WithoutCancel(ctx))
	})}, nil
}

func countInbox(ctx context.Context, pool *pgxpool.Pool, eventIDs []string) (int, error) {
	var counted int
	err := pool.QueryRow(ctx, "SELECT count(*) FROM inbox WHERE event_id = ANY($1)", eventIDs).Scan(&counted)
	return counted, err
}
