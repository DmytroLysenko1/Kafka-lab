package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const countWaiting = `SELECT count(*) FROM outbox WHERE published_at IS NULL`

// watch reads the two numbers an operator watches: how much the outbox is holding, and
// whether the cluster still has every replica it is supposed to have.
func watch(ctx context.Context, pool *pgxpool.Pool, topic string, started time.Time, record func(sample)) {
	ticker := time.NewTicker(sampleEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reading := sample{
				at: time.Since(started),
			}

			reading.backlog, reading.backlogKnown = count(ctx, pool, countWaiting)
			reading.underReplicated, reading.leaderless, reading.clusterKnown = replication(ctx, topic)
			record(reading)
		}
	}
}

// count answers with a number and whether it is one. A failed read used to come back as
// -1, which every comparison downstream read as "less than anything" — the drain check
// would have called a database outage a drained queue.
func count(ctx context.Context, pool *pgxpool.Pool, query string) (int, bool) {
	bounded, cancel := context.WithTimeout(ctx, requestBudget)
	defer cancel()

	var rows int
	if err := pool.QueryRow(bounded, query).Scan(&rows); err != nil {
		return 0, false
	}
	return rows, true
}

// replication reads the topic's metadata rather than parsing the output of kafka-topics.sh.
// That script prints connection warnings and an empty result when brokers are missing, and
// counting the lines of an empty result reports a perfectly healthy cluster at the exact
// moment two thirds of it is gone — which is what the first version of this run did.
func replication(ctx context.Context, topic string) (underReplicated, leaderless int, known bool) {
	bounded, cancel := context.WithTimeout(ctx, requestBudget)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(os.Getenv("KAFKA_BROKERS"), ",")...))
	if err != nil {
		return 0, 0, false
	}
	defer client.Close()

	topics, err := kadm.NewClient(client).ListTopics(bounded, topic)
	if err != nil {
		return 0, 0, false
	}

	details, found := topics[topic]
	// An answer that does not mention the topic is not an answer about the topic. Counting
	// the partitions of an empty response reports a healthy cluster at exactly the moment
	// there is nobody left to describe it — this run has now made that mistake twice, once
	// by parsing an empty CLI result and once by ranging over an empty metadata response.
	if !found || details.Err != nil || len(details.Partitions) == 0 {
		return 0, 0, false
	}

	for _, partition := range details.Partitions {
		switch {
		case partition.Leader < 0:
			leaderless++
		case len(partition.ISR) < len(partition.Replicas):
			underReplicated++
		}
	}
	return underReplicated, leaderless, true
}

// load takes payments through the real API at a steady rate, and records what the caller
// saw rather than what the service logged: the claim under test is that the front door
// keeps answering while the events pile up behind it.
func load(ctx context.Context, s *settings, started time.Time, record func(attempt)) {
	interval := time.Second / time.Duration(s.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	client := &http.Client{
		Timeout: requestBudget,
	}
	sent := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sent++
			record(attempt{
				at:       time.Since(started),
				accepted: pay(ctx, client, s, sent),
			})
		}
	}
}

func pay(ctx context.Context, client *http.Client, s *settings, number int) bool {
	body, err := json.Marshal(map[string]any{
		"merchant_id":  s.merchant,
		"amount_minor": 1,
		"currency":     "EUR",
	})
	if err != nil {
		return false
	}

	bounded, cancel := context.WithTimeout(ctx, requestBudget)
	defer cancel()

	request, err := http.NewRequestWithContext(bounded, http.MethodPost, s.api+"/payments", bytes.NewReader(body)) //nolint:gosec // the api address is this experiment's own flag
	if err != nil {
		return false
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-API-Key", s.apiKey)
	request.Header.Set("Idempotency-Key", fmt.Sprintf("%s-%d", s.merchant, number))

	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer func() {
		_ = response.Body.Close()
	}()

	return response.StatusCode == http.StatusCreated
}

// drain waits for the outbox to empty once the cluster is whole again, and reports how
// long that took from the moment the run stopped taking payments.
func drain(ctx context.Context, pool *pgxpool.Pool, started time.Time) (time.Duration, error) {
	stoppedTaking := time.Now()

	bounded, cancel := context.WithTimeout(ctx, drainDeadline)
	defer cancel()

	ticker := time.NewTicker(sampleEvery)
	defer ticker.Stop()

	for {
		select {
		case <-bounded.Done():
			return time.Since(started), fmt.Errorf("%w within %s", errDrain, drainDeadline)
		case <-ticker.C:
			if waiting, known := count(bounded, pool, countWaiting); known && waiting == 0 {
				return time.Since(stoppedTaking), nil
			}
		}
	}
}
