// Package stand is the shared harness behind exp-05, exp-06 and exp-07. The three
// experiments differ in one thing — where the offset is committed relative to the write —
// so everything else they need lives here once: producing a run, draining it, writing it
// down, and deciding whether the run demonstrated what it set out to.
package stand

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"
)

// Mode is the ordering decision the three experiments exist to compare.
type Mode string

const (
	AtMostOnce  Mode = "at-most-once"
	AtLeastOnce Mode = "at-least-once"
	Inbox       Mode = "inbox"
)

// CommitsFirst is the whole difference between at-most-once and the other two: storing the
// offset before the payment is written means a crash in between loses the payment, and
// storing it after means the crash replays it.
func (m Mode) CommitsFirst() bool {
	return m == AtMostOnce
}

// Expects is what a mode must demonstrate when the consumer dies in the middle. A run that
// produces anything else has not proved its point, whatever the totals look like.
func (m Mode) Expects() Outcome {
	switch m {
	case AtMostOnce:
		return OutcomeLost
	case AtLeastOnce:
		return OutcomeDuplicate
	default:
		return OutcomeExactly
	}
}

// Experiment is what one of the three programs is: a name for its messages, the ordering it
// demonstrates, and the topic it owns.
type Experiment struct {
	Name  string
	Mode  Mode
	Topic string
}

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"

	maxPayments = 1_000_000
	// PollBatch is also the size of the window a crash can lose or replay, which is why it
	// is fixed rather than left to the client's defaults: the published numbers are
	// multiples of it, and a reader has to be able to see why.
	PollBatch = 50
)

var phases = []string{"produce", "consume", "verify"}

var (
	ErrPhase   = errors.New("stand: -phase must be produce, consume or verify")
	ErrRunID   = errors.New("stand: -run-id is required, so a rerun cannot read an earlier run's rows")
	ErrShape   = errors.New("stand: -payments out of range")
	ErrPartial = errors.New("stand: the cluster did not accept every payment")
)

// Settings is one phase of one run.
type Settings struct {
	Experiment
	Brokers  string
	Database string
	Phase    string
	RunID    string
	Payments int
	DieAfter int
	Timeout  time.Duration
}

// Main is the whole of each experiment's main(): the three programs differ in the
// Experiment they hand over and in nothing else.
func Main(exp Experiment) {
	if err := run(exp); err != nil {
		slog.Error(exp.Name+" failed", "error", err)
		os.Exit(1)
	}
}

func run(exp Experiment) error {
	cfg := Settings{Experiment: exp}
	flag.StringVar(&cfg.Brokers, "brokers", defaultBrokers, "comma-separated bootstrap brokers")
	flag.StringVar(&cfg.Database, "database-url", os.Getenv("DATABASE_URL"), "postgres connection string")
	flag.StringVar(&cfg.Phase, "phase", "", "produce, consume or verify")
	flag.StringVar(&cfg.RunID, "run-id", "", "identifier shared by the phases of one run")
	flag.IntVar(&cfg.Payments, "payments", 1000, "payments produced in this run")
	flag.IntVar(&cfg.DieAfter, "die-after", 0, "SIGKILL this process once it has handled this many payments; 0 never dies")
	flag.DurationVar(&cfg.Timeout, "timeout", 3*time.Minute, "deadline for this phase")
	flag.Parse()

	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	return measure(ctx, &cfg, os.Stdout)
}

func (cfg *Settings) validate() error {
	switch {
	case !slices.Contains(phases, cfg.Phase):
		return ErrPhase
	case cfg.RunID == "":
		return ErrRunID
	case cfg.Payments <= 0 || cfg.Payments > maxPayments:
		return fmt.Errorf("%w: -payments is %d", ErrShape, cfg.Payments)
	case cfg.DieAfter < 0 || cfg.DieAfter > cfg.Payments:
		return fmt.Errorf("%w: -die-after is %d", ErrShape, cfg.DieAfter)
	}
	return nil
}
