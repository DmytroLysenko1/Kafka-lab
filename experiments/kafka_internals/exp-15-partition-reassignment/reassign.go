package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var errReassign = errors.New("exp-15: reassignment")

const (
	verifyEvery    = time.Second
	commandTimeout = 30 * time.Second
	planPath       = "/tmp/exp15-plan.json"
	admin          = "kafka-lab-kafka3"
)

// move is what the reassignment cost, as the cluster reported it.
type move struct {
	startedAt time.Duration
	finished  time.Duration
	took      time.Duration
}

type plan struct {
	Version    int              `json:"version"`
	Partitions []plannedReplica `json:"partitions"`
}

type plannedReplica struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Replicas  []int  `json:"replicas"`
}

// reassign waits, then moves every partition onto all three brokers — the replication
// factor change topicctl refuses to make — and waits for the cluster to say it is done.
func reassign(ctx context.Context, s *settings, started time.Time) (move, error) {
	select {
	case <-ctx.Done():
		return move{}, ctx.Err()
	case <-time.After(s.moveAfter):
	}

	if err := writePlan(ctx, s.topic); err != nil {
		return move{}, err
	}

	startedAt := time.Since(started)
	if err := execute(ctx, s.throttle); err != nil {
		return move{}, err
	}

	finished, err := waitForVerify(ctx)
	if err != nil {
		return move{}, err
	}
	return move{startedAt: startedAt, finished: time.Since(started), took: finished}, nil
}

func writePlan(ctx context.Context, topic string) error {
	wanted := plan{Version: 1}
	for partition := range 3 {
		wanted.Partitions = append(wanted.Partitions, plannedReplica{
			Topic:     topic,
			Partition: partition,
			Replicas:  []int{1, 2, 3},
		})
	}
	encoded, err := json.Marshal(wanted)
	if err != nil {
		return err
	}

	// The plan has to live where the tool runs, which is inside a broker container.
	write := exec.CommandContext(ctx, "docker", "exec", "-i", admin, "sh", "-c", "cat > "+planPath)
	write.Stdin = strings.NewReader(string(encoded))
	if output, err := write.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: write the plan: %w: %s", errReassign, err, output)
	}
	return nil
}

func execute(ctx context.Context, throttle int) error {
	arguments := []string{"--bootstrap-server", "localhost:9092", "--reassignment-json-file", planPath, "--execute"}
	if throttle > 0 {
		arguments = append(arguments, "--throttle", strconv.Itoa(throttle))
	}
	_, err := reassignTool(ctx, arguments...)
	return err
}

// waitForVerify polls until the cluster reports the move complete. Verify is also what
// removes the throttle it set: a run that stopped at execute would leave every later
// experiment replicating at a megabyte a second and wondering why.
func waitForVerify(ctx context.Context) (time.Duration, error) {
	started := time.Now()
	ticker := time.NewTicker(verifyEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return time.Since(started), fmt.Errorf("%w: the move did not finish within the run", errReassign)
		case <-ticker.C:
			output, err := reassignTool(ctx, "--bootstrap-server", "localhost:9092",
				"--reassignment-json-file", planPath, "--verify")
			if err != nil {
				continue
			}
			if strings.Contains(string(output), "is complete") || strings.Contains(string(output), "successfully completed") {
				return time.Since(started), nil
			}
		}
	}
}

func reassignTool(ctx context.Context, arguments ...string) ([]byte, error) {
	bounded, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()

	all := append([]string{"exec", admin, "/opt/kafka/bin/kafka-reassign-partitions.sh"}, arguments...)
	command := exec.CommandContext(bounded, "docker", all...) //nolint:gosec // every argument comes from this file or this experiment's own flags

	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%w: %v: %w: %s", errReassign, arguments, err, output)
	}
	return output, nil
}

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

func report(out io.Writer, s *settings, timed []latency, made move, copied int64) error {
	throttle := "none"
	if s.throttle > 0 {
		throttle = fmt.Sprintf("%d KiB/s", s.throttle/1024)
	}

	phases := []struct {
		name  string
		from  time.Duration
		until time.Duration
	}{
		{name: "before the move", from: 0, until: made.startedAt},
		{name: "while replicas copied", from: made.startedAt, until: made.finished},
		{name: "after it finished", from: made.finished, until: s.window},
	}

	written := &lines{out: out}
	written.printf("throttle: %s\n", throttle)
	written.printf("the move took %s and put %s more on disk across the cluster\n\n",
		made.took.Round(100*time.Millisecond), mib(copied))
	written.printf("%-24s %-12s %8s %10s %10s %10s\n", "phase", "seconds", "writes", "median", "p95", "worst")

	for _, span := range phases {
		count, median, p95, worst := quantiles(timed, span.from, span.until)
		written.printf("%-24s %-12s %8d %10s %10s %10s\n",
			span.name,
			fmt.Sprintf("%.0f–%.0f", span.from.Seconds(), span.until.Seconds()),
			count,
			median.Round(100*time.Microsecond),
			p95.Round(100*time.Microsecond),
			worst.Round(100*time.Microsecond))
	}
	return written.err
}

// onDisk asks the brokers how much of this topic they are actually holding. The nominal
// size of what was produced is not the same number: the brokers store what the producer
// sent them, compressed, and a run that trusts its own arithmetic measures a move that
// never happened.
func onDisk(ctx context.Context, topic string) (int64, error) {
	bounded, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
	if err != nil {
		return 0, err
	}
	defer client.Close()

	dirs, err := kadm.NewClient(client).DescribeAllLogDirs(bounded, nil)
	if err != nil {
		return 0, fmt.Errorf("%w: log dirs: %w", errReassign, err)
	}
	if err := answeredEveryBroker(dirs); err != nil {
		return 0, err
	}
	return storedBytes(dirs, topic), nil
}

// answeredEveryBroker refuses a description in which any broker failed. Such a broker
// contributes nothing to the sum, which understates the bytes copied and inflates every
// rate derived from them — quietly, since the total still looks like a measurement.
func answeredEveryBroker(dirs kadm.DescribedAllLogDirs) error {
	if len(dirs) == 0 {
		return fmt.Errorf("%w: no broker described its log dirs", errReassign)
	}
	for broker, described := range dirs {
		if err := described.Error(); err != nil {
			return fmt.Errorf("%w: log dirs of broker %d: %w", errReassign, broker, err)
		}
	}
	return nil
}

func storedBytes(dirs kadm.DescribedAllLogDirs, topic string) int64 {
	var stored int64
	dirs.Each(func(dir kadm.DescribedLogDir) {
		for _, partition := range dir.Topics[topic] {
			stored += partition.Size
		}
	})
	return stored
}
