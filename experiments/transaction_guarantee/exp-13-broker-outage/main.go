// exp-13 takes payments through the real API while brokers are killed underneath the
// relay, and watches three things at once: whether the front door keeps answering, how far
// the outbox backs up, and how long it takes to drain once the cluster is whole again.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errVictims = errors.New("exp-13: at least one broker has to be killed for this to measure anything")
	errDrain   = errors.New("exp-13: the outbox never drained")
)

const (
	sampleEvery   = 500 * time.Millisecond
	drainDeadline = 2 * time.Minute
	requestBudget = 5 * time.Second
)

type settings struct {
	api       string
	apiKey    string
	merchant  string
	rate      int
	window    time.Duration
	killAfter time.Duration
	downFor   time.Duration
	victims   string
	topic     string
}

// sample is one reading of what an operator would be watching: how much the outbox holds,
// and what state the topic's partitions are in. Leaderless partitions are counted apart
// from under-replicated ones — they are different problems, and only one of them is what
// "under-replicated" means.
type sample struct {
	at              time.Duration
	backlog         int
	backlogKnown    bool
	underReplicated int
	leaderless      int
	clusterKnown    bool
}

// attempt is one payment as the caller saw it: accepted, refused, or never answered.
type attempt struct {
	at       time.Duration
	accepted bool
}

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("exp-13 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var s settings
	flag.StringVar(&s.api, "api", "http://localhost:8081", "the payments api")
	flag.StringVar(&s.apiKey, "api-key", "lab-key", "its api key")
	flag.StringVar(&s.merchant, "merchant", "exp13", "merchant to charge")
	flag.IntVar(&s.rate, "rate", 10, "payments per second")
	flag.DurationVar(&s.window, "window", 90*time.Second, "how long to keep taking payments")
	flag.DurationVar(&s.killAfter, "kill-after", 20*time.Second, "when to kill the brokers")
	flag.DurationVar(&s.downFor, "down-for", 30*time.Second, "how long they stay dead")
	flag.StringVar(&s.victims, "victims", "1", "broker ids to kill, comma separated")
	flag.StringVar(&s.topic, "topic", "exp13.payments", "the topic whose replication this run watches")
	flag.Parse()

	if strings.TrimSpace(s.victims) == "" {
		return errVictims
	}
	return measure(context.Background(), &s, os.Stdout)
}

func measure(ctx context.Context, s *settings, out io.Writer) error {
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer pool.Close()

	running, stop := context.WithTimeout(ctx, s.window)
	defer stop()

	started := time.Now()
	var (
		wg       sync.WaitGroup
		samples  []sample
		attempts []attempt
		mu       sync.Mutex
	)

	wg.Go(func() {
		watch(running, pool, s.topic, started, func(reading sample) {
			mu.Lock()
			samples = append(samples, reading)
			mu.Unlock()
		})
	})

	wg.Go(func() {
		load(running, s, started, func(made attempt) {
			mu.Lock()
			attempts = append(attempts, made)
			mu.Unlock()
		})
	})

	killed, revived, err := outage(running, s, started)
	if err != nil {
		return err
	}

	wg.Wait()

	drained, err := drain(ctx, pool, started)
	if err != nil {
		return err
	}

	return report(out, s, samples, attempts, killed, revived, drained)
}

// outage does the killing from inside the run rather than from the shell around it, so the
// moment a broker died is the same clock the samples are on — a kill timed from outside
// lands somewhere between two readings and the report has to guess where.
func outage(ctx context.Context, s *settings, started time.Time) (killed, revived time.Duration, err error) {
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case <-time.After(s.killAfter):
	}
	if err := docker(ctx, "kill", s.victims); err != nil {
		return 0, 0, err
	}
	killed = time.Since(started)

	select {
	case <-ctx.Done():
		return killed, 0, ctx.Err()
	case <-time.After(s.downFor):
	}
	if err := docker(ctx, "start", s.victims); err != nil {
		return killed, 0, err
	}
	return killed, time.Since(started), nil
}

func docker(ctx context.Context, action, victims string) error {
	for id := range strings.SplitSeq(victims, ",") {
		container := "kafka-lab-kafka" + strings.TrimSpace(id)
		command := exec.CommandContext(ctx, "docker", action, container) //nolint:gosec // container names are built from this experiment's own flag, not from input
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("docker %s %s: %w: %s", action, container, err, output)
		}
	}
	return nil
}
