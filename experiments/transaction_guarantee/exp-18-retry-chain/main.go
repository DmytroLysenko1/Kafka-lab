// exp-18 measures what the retry chain buys when one merchant's row is held by another
// transaction, using the service's own consumer, detours and replay. The cells differ in
// one thing each: whether contention is told apart from any other database failure, and
// whether the lock outlasts the chain.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

var (
	errUnknownCell = errors.New("exp-18: cell must be a, b or c")
	errBadShape    = errors.New("exp-18: the run's shape is inconsistent")
)

const (
	cellBlind     = "a"
	cellReleased  = "b"
	cellExhausted = "c"
)

// settings is one cell. hold is how long the merchant stays locked; cell C ignores it and
// holds until every contended payment has been archived, since that is the case it is for.
type settings struct {
	cell         string
	hold         time.Duration
	payments     int
	hotEvery     int
	firstEventID int64
	budget       time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("exp-18 failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var s settings
	flag.StringVar(&s.cell, "cell", "", "a: no chain · b: chain, lock released after -hold · c: chain, lock held until every contended payment is archived")
	flag.DurationVar(&s.hold, "hold", 30*time.Second, "how long the merchant's row stays locked in cells a and b")
	flag.IntVar(&s.payments, "payments", 600, "payments to produce")
	flag.IntVar(&s.hotEvery, "hot-every", 100, "every n-th payment is for the locked merchant")
	flag.Int64Var(&s.firstEventID, "first-event-id", 0, "event ids start here; each cell needs its own range or the inbox would call one cell's payment a duplicate of another's")
	flag.DurationVar(&s.budget, "budget", 3*time.Minute, "how long the cell may take before it is reported as never drained")
	flag.Parse()

	if err := s.validate(); err != nil {
		return err
	}
	return measureCell(context.Background(), &s)
}

func (s *settings) validate() error {
	switch {
	case s.cell != cellBlind && s.cell != cellReleased && s.cell != cellExhausted:
		return fmt.Errorf("%w: %q", errUnknownCell, s.cell)
	case s.payments <= 0 || s.hotEvery <= 0 || s.hotEvery > s.payments:
		return fmt.Errorf("%w: %d payments, every %d-th contended", errBadShape, s.payments, s.hotEvery)
	case s.firstEventID <= 0:
		return fmt.Errorf("%w: -first-event-id is required", errBadShape)
	}
	return nil
}

func (s *settings) chained() bool {
	return s.cell != cellBlind
}

func (s *settings) topic(suffix string) string {
	return "exp18." + s.cell + "." + suffix
}

func (s *settings) group(suffix string) string {
	return "exp18-" + s.cell + "-" + suffix
}

func (s *settings) hotMerchant() string {
	return "exp18-" + s.cell + "-hot"
}

// merchantFor spreads the healthy payments over nine merchants, so the locked one is one
// merchant among several rather than the only other thing in the topic.
func (s *settings) merchantFor(i int) string {
	if s.isHot(i) {
		return s.hotMerchant()
	}
	return fmt.Sprintf("exp18-%s-m%d", s.cell, i%9)
}

func (s *settings) isHot(i int) bool {
	return i%s.hotEvery == s.hotEvery/2
}

func (s *settings) hotCount() int {
	count := 0
	for i := range s.payments {
		if s.isHot(i) {
			count++
		}
	}
	return count
}

func brokers() []string {
	return strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
}
