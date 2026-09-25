// dlq-replayer sends the dead letters of one class back to the start of the retry chain,
// once whatever put them there has been dealt with. It is run by hand and exits when it
// has read the dead letter topic up to where it ended when the replay began.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/metrics"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/env"
)

var errNotATier = errors.New("dlq-replayer: letters are replayed only into this group's retry tiers")

const retryTierPrefix = "payments-consumer.retry."

// main keeps a skeleton of its own rather than lifecycle.Main: the services share one
// outcome — running, or stopped with a reason — while this tool has three, and says which
// in its exit code: 2 when it never started, 1 when it started and failed part-way, with
// what it had replayed by then as fields an operator can query rather than as prose.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	// os.Exit skips deferred calls, so it stays out of the function that holds them.
	if code := start(logger); code != 0 {
		os.Exit(code)
	}
}

func start(logger *slog.Logger) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	request, brokers, err := load()
	if err != nil {
		logger.ErrorContext(ctx, "dlq replay not started", "error", err)
		return 2
	}

	report, err := replay(ctx, brokers, request)
	if err != nil {
		logger.ErrorContext(ctx, "dlq replay stopped", "error", err, "replayed", report.Replayed, "skipped", report.Skipped)
		return 1
	}
	logger.InfoContext(ctx, "dlq replay finished",
		"class", request.Class,
		"into", request.Into,
		"replayed", report.Replayed,
		"skipped", report.Skipped,
	)
	return 0
}

func replay(ctx context.Context, brokers []string, request kafka.ReplayRequest) (kafka.ReplayReport, error) {
	detours, err := kafka.NewDetours(brokers, request.DeadLetterTopic, metrics.Discard{})
	if err != nil {
		return kafka.ReplayReport{}, err
	}
	defer detours.Close()

	return kafka.Replay(ctx, brokers, request, detours)
}

// load defaults the class to the only one a replay can fix by itself: an undecodable record
// will be just as undecodable the second time, and a refused one needs its producer fixed,
// not another attempt. The others can still be named, after that fix has shipped.
func load() (kafka.ReplayRequest, []string, error) {
	var request kafka.ReplayRequest
	class := flag.String("class", string(kafka.ClassExhausted), "dead letter class to replay: exhausted, refused or undecodable")
	flag.StringVar(&request.DeadLetterTopic, "from", "payments-consumer.dlq", "dead letter topic")
	flag.StringVar(&request.Into, "into", "payments-consumer.retry.5s", "first tier of the chain the letters go back into")
	flag.Parse()

	parsed, err := kafka.ParseClass(*class)
	if err != nil {
		return kafka.ReplayRequest{}, nil, err
	}
	request.Class = parsed
	request.Group = "payments-consumer.dlq-replayer." + *class
	// A replay goes back to the group's own chain and nowhere else: into payments.main it
	// would reach every group reading that topic, and one without an inbox counts it twice.
	if !strings.HasPrefix(request.Into, retryTierPrefix) {
		return kafka.ReplayRequest{}, nil, fmt.Errorf("%w: %q", errNotATier, request.Into)
	}

	var read env.Reader
	brokers := read.List("KAFKA_BROKERS")
	return request, brokers, read.Err()
}
