package main

import (
	"fmt"
	"io"
	"time"
)

// lines writes the report and keeps the first error rather than checking every write.
type lines struct {
	out io.Writer
	err error
}

func (l *lines) printf(format string, args ...any) {
	if l.err != nil {
		return
	}
	_, l.err = fmt.Fprintf(l.out, format, args...)
}

// phase is a stretch of the run with a name an operator would recognise.
type phase struct {
	name  string
	from  time.Duration
	until time.Duration
}

func report(out io.Writer, s *settings, samples []sample, attempts []attempt, killed, revived, drained time.Duration) error {
	phases := []phase{
		{name: "before the kill", from: 0, until: killed},
		{name: "brokers " + s.victims + " down", from: killed, until: revived},
		{name: "after they came back", from: revived, until: s.window},
	}

	written := &lines{out: out}
	written.printf("%-24s %-12s %10s %10s %14s %12s %10s\n",
		"phase", "seconds", "accepted", "refused", "backlog (max)", "under-repl", "no leader")

	for _, span := range phases {
		accepted, refused := countAttempts(attempts, span)
		state := worst(samples, span)
		written.printf("%-24s %-12s %10d %10d %14d %12s %10s\n",
			span.name,
			fmt.Sprintf("%.0f–%.0f", span.from.Seconds(), span.until.Seconds()),
			accepted, refused, state.backlog,
			partitions(state.underReplicated, state.clusterKnown), partitions(state.leaderless, state.clusterKnown))
	}

	steady := worst(samples, phases[0]).backlog
	written.printf("\npeak backlog: %d records\n", peak(samples))
	if caught, ok := caughtUp(samples, revived, steady); ok {
		written.printf("backlog back to its pre-outage level (%d records) %s after the brokers came back\n",
			steady, caught.Round(100*time.Millisecond))
	} else {
		written.printf("backlog never returned to its pre-outage level (%d records) while the run lasted\n", steady)
	}
	written.printf("outbox drained %s after the run stopped taking payments\n", drained.Round(100*time.Millisecond))
	return written.err
}

func countAttempts(attempts []attempt, span phase) (accepted, refused int) {
	for _, made := range attempts {
		if made.at < span.from || made.at >= span.until {
			continue
		}
		if made.accepted {
			accepted++
			continue
		}
		refused++
	}
	return accepted, refused
}

// worst reports the highest of each number seen in a phase: an average would hide the
// moment the run is about. A reading nobody could take is reported as unknown rather than
// folded into a maximum, where it would read as zero — the opposite of what it means.
func worst(samples []sample, span phase) sample {
	highest := sample{}
	for _, reading := range samples {
		if reading.at < span.from || reading.at >= span.until {
			continue
		}
		if reading.backlogKnown {
			highest.backlog = max(highest.backlog, reading.backlog)
			highest.backlogKnown = true
		}
		if reading.clusterKnown {
			highest.underReplicated = max(highest.underReplicated, reading.underReplicated)
			highest.leaderless = max(highest.leaderless, reading.leaderless)
			highest.clusterKnown = true
		}
	}
	return highest
}

func partitions(count int, known bool) string {
	if !known {
		return "unknown"
	}
	return fmt.Sprintf("%d", count)
}

// caughtUp is the number the outbox exists to make small: how long after the cluster came
// back the relay had worked off what piled up while it was away. The target is the backlog
// the run had before the kill, not zero — payments keep arriving throughout, so a few
// records are always in flight between one sweep and the next.
func caughtUp(samples []sample, revived time.Duration, steady int) (time.Duration, bool) {
	for _, reading := range samples {
		if reading.at > revived && reading.backlogKnown && reading.backlog <= steady {
			return reading.at - revived, true
		}
	}
	return 0, false
}

func peak(samples []sample) int {
	highest := 0
	for _, reading := range samples {
		if reading.backlogKnown {
			highest = max(highest, reading.backlog)
		}
	}
	return highest
}
