// Command askdocs answers a question about Kafka-lab from the lab's own documentation, and
// says which sections the answer rests on.
//
//	askdocs -root .. "What happens to the outbox while Kafka is down?"
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/application/ask"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/infrastructure/bm25"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/infrastructure/claude"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/infrastructure/markdown"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/interfaces/cli"
)

// Exit codes from sysexits(3), so a script can tell "try again later" from "fix the setup".
const (
	exitSuccess    = 0
	exitFailure    = 1
	exitUsage      = 64
	exitNoInput    = 66
	exitTryLater   = 75
	exitBadSetting = 78
	// 128 + SIGINT, what a shell reports for a command stopped with Ctrl-C; SIGTERM ends the
	// same way.
	exitInterrupted = 130
)

var errBadFlags = errors.New("askdocs: unusable command line")

type options struct {
	root         string
	retrieveOnly bool
	question     string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	os.Exit(run(logger, os.Args[1:], os.Stdout))
}

func run(logger *slog.Logger, args []string, out io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := respond(ctx, logger, args, out)
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		logger.Error("no answer", "error", err)
	}
	return exitCode(err)
}

// respond checks the command line and the question before the corpus is read: a mistyped
// question is answered in milliseconds, not after indexing three hundred sections.
func respond(ctx context.Context, logger *slog.Logger, args []string, out io.Writer) error {
	settings, err := parse(args)
	if err != nil {
		return err
	}
	question, err := docs.NewQuestion(settings.question)
	if err != nil {
		return err
	}
	questions, err := provideAsk(ctx, logger, settings.root)
	if err != nil {
		return err
	}
	if settings.retrieveOnly {
		evidence, err := questions.Evidence(ctx, question)
		if err != nil {
			return err
		}
		return cli.WriteEvidence(out, evidence)
	}
	answer, err := questions.Execute(ctx, question)
	if err != nil {
		return err
	}
	return cli.WriteAnswer(out, answer)
}

func parse(args []string) (options, error) {
	var settings options
	flags := flag.NewFlagSet("askdocs", flag.ContinueOnError)
	flags.StringVar(&settings.root, "root", ".", "the Kafka-lab checkout whose documentation is searched")
	flags.BoolVar(&settings.retrieveOnly, "retrieve-only", false, "print the sections the question would be answered from, without calling the model")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return options{}, err
		}
		return options{}, fmt.Errorf("%w: %w", errBadFlags, err)
	}
	settings.question = strings.Join(flags.Args(), " ")
	return settings, nil
}

func provideAsk(ctx context.Context, logger *slog.Logger, root string) (*ask.Ask, error) {
	// The documentation the lab vouches for: the top-level README, the write-ups in docs/, each
	// experiment's README, and this command's own, so it can answer for its decisions too.
	sections, err := markdown.Load(ctx, os.DirFS(root), "README.md", "docs", "experiments", "askdocs/README.md")
	if err != nil {
		return nil, err
	}
	logger.InfoContext(ctx, "documentation indexed",
		"root", root,
		"sections", len(sections),
		"bytes", lo.SumBy(sections, docs.Section.Size),
	)
	// The global tracer does nothing until a process installs a provider; this command
	// installs none, so its spans go nowhere unless askdocs is embedded somewhere that does.
	return ask.New(bm25.New(sections), claude.New(logger), otel.Tracer("askdocs")), nil
}

// exitCode is the one place a failure becomes a process outcome.
func exitCode(err error) int {
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return exitSuccess
	case errors.Is(err, errBadFlags), errors.Is(err, docs.ErrEmptyQuestion), errors.Is(err, docs.ErrQuestionTooLong):
		return exitUsage
	case errors.Is(err, markdown.ErrNoDocuments), errors.Is(err, markdown.ErrRootMissing), errors.Is(err, docs.ErrNothingRelevant):
		return exitNoInput
	case errors.Is(err, context.Canceled):
		return exitInterrupted
	case errors.Is(err, docs.ErrModelUnavailable):
		return exitTryLater
	case errors.Is(err, docs.ErrRequestRejected):
		return exitBadSetting
	default:
		return exitFailure
	}
}
