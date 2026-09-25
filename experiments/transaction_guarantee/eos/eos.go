// Package eos is the shared harness behind exp-10a, 10b and 10c: a read-process-write loop
// run inside Kafka transactions, crashed in the middle of one, and read back in the ways
// that tell the three experiments apart. What Kafka's exactly-once covers, and where it
// stops, is decided by what each experiment reads — the loop is the same.
package eos

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"
)

// Experiment is what one of the three programs is.
type Experiment struct {
	Name   string
	Input  string
	Output string
	// WriteDB makes the loop write each payment to Postgres as well — inside the loop, but
	// outside anything the Kafka transaction can roll back.
	WriteDB bool
	// Claim is what this experiment must demonstrate; the verify phase holds it to that.
	Claim Claim
}

// Claim names the three things these experiments set out to show.
type Claim int

const (
	// KafkaExactlyOnce: a read_committed reader of the output sees every payment once.
	KafkaExactlyOnce Claim = iota
	// DatabaseNotCovered: the Kafka output is exact and the database rows are not.
	DatabaseNotCovered
	// AbortedVisibleUncommitted: a read_uncommitted reader is handed the aborted records.
	AbortedVisibleUncommitted
)

const (
	defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"
	maxPayments    = 1_000_000
	// Batch is the size of the transaction a crash lands in, and so the size of what is
	// rolled back: the published numbers are this, and a reader has to be able to see why.
	Batch     = 50
	readStall = 15 * time.Second
)

var phases = []string{"seed", "process", "verify"}

var (
	ErrPhase           = errors.New("eos: -phase must be seed, process or verify")
	ErrRunID           = errors.New("eos: -run-id is required, so a rerun cannot read an earlier run's output")
	ErrShape           = errors.New("eos: -payments or -die-after out of range")
	ErrPartial         = errors.New("eos: the cluster did not accept every input payment")
	ErrNotDemonstrated = errors.New("eos: the run did not show what it exists to show")
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

func (cfg *Settings) brokers() []string { return strings.Split(cfg.Brokers, ",") }

// transactionalID is fixed for a run and different across runs. Fixed, because the process
// that replaces a crashed one must present the same id: that is how the broker learns the
// old transaction is dead and aborts it. Different across runs, so a rerun does not inherit
// an earlier run's producer epoch.
func (cfg *Settings) transactionalID() string { return cfg.Name + "-" + cfg.RunID }
func (cfg *Settings) group() string           { return cfg.Name + "-" + cfg.RunID }

// Main is the whole of each experiment's main().
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
	flag.StringVar(&cfg.Phase, "phase", "", strings.Join(phases, ", "))
	flag.StringVar(&cfg.RunID, "run-id", "", "identifier shared by the phases of one run")
	flag.IntVar(&cfg.Payments, "payments", 1000, "payments seeded onto the input topic")
	flag.IntVar(&cfg.DieAfter, "die-after", 0, "SIGKILL this process mid-transaction once it has handled this many; 0 never dies")
	flag.DurationVar(&cfg.Timeout, "timeout", 4*time.Minute, "deadline for this phase")
	flag.Parse()

	switch {
	case !slices.Contains(phases, cfg.Phase):
		return ErrPhase
	case cfg.RunID == "":
		return ErrRunID
	case cfg.Payments <= 0 || cfg.Payments > maxPayments || cfg.DieAfter < 0 || cfg.DieAfter > cfg.Payments:
		return fmt.Errorf("%w: -payments %d, -die-after %d", ErrShape, cfg.Payments, cfg.DieAfter)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	switch cfg.Phase {
	case "seed":
		return seed(ctx, &cfg)
	case "process":
		return process(ctx, &cfg)
	default:
		return verify(ctx, &cfg, os.Stdout)
	}
}
