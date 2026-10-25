package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/infrastructure/markdown"
)

var errUnexpected = errors.New("something nobody classified")

func TestExitCode(t *testing.T) {
	type args struct{ err error }
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr error
	}{
		{name: "a blank question is a usage error", args: args{err: docs.ErrEmptyQuestion}, want: exitUsage},
		{name: "an overlong question is a usage error", args: args{err: docs.ErrQuestionTooLong}, want: exitUsage},
		{name: "no matching section means no input to answer from", args: args{err: docs.ErrNothingRelevant}, want: exitNoInput},
		{name: "no documentation under the root means no input", args: args{err: fmt.Errorf("load: %w", markdown.ErrNoDocuments)}, want: exitNoInput},
		{
			name: "an API that stayed overloaded says try later, through the adapter's wrapping",
			args: args{err: fmt.Errorf("ask claude-opus-5-5: %w", errors.Join(docs.ErrModelUnavailable, errUnexpected))},
			want: exitTryLater,
		},
		{
			name: "a rejected request says fix the setup",
			args: args{err: fmt.Errorf("ask claude-opus-5-5: %w", errors.Join(docs.ErrRequestRejected, errUnexpected))},
			want: exitBadSetting,
		},
		{name: "a refusal is a plain failure", args: args{err: docs.ErrRefused}, want: exitFailure},
		{name: "a cut-off answer is a plain failure", args: args{err: docs.ErrTruncated}, want: exitFailure},
		{name: "Ctrl-C is an interruption, not a failure", args: args{err: fmt.Errorf("ask claude-opus-5-5: %w", context.Canceled)}, want: exitInterrupted},
		{name: "asking for help is not a failure", args: args{err: flag.ErrHelp}, want: exitSuccess},
		{name: "an unknown flag is a usage error", args: args{err: fmt.Errorf("%w: %w", errBadFlags, errUnexpected)}, want: exitUsage},
		{name: "a root that does not exist means no input", args: args{err: fmt.Errorf("load: %w", markdown.ErrRootMissing)}, want: exitNoInput},
		{name: "an answer with no text is a plain failure", args: args{err: docs.ErrEmptyAnswer}, want: exitFailure},
		{name: "a model failure nothing classifies is a plain failure", args: args{err: errors.Join(docs.ErrModelFailed, errUnexpected)}, want: exitFailure},
		{name: "anything unclassified is a plain failure", args: args{err: errUnexpected}, want: exitFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, exitCode(tt.args.err)); diff != "" {
				t.Errorf("exit code mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func docsRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"README.md":                           "# Kafka-lab\nA lab for the outbox relay.\n",
		"docs/runbook.md":                     "# Runbook\n## Kafka down\nThe outbox backlog grows and drains.\n",
		"experiments/exp-13-outage/README.md": "# exp-13\nBrokers stopped; the outbox held.\n",
		"askdocs/README.md":                   "# askdocs\nIt answers from the docs.\n",
	}
	for name, content := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatalf("fixture: %v", err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("fixture: %v", err)
		}
	}
	return root
}

// run is the whole command short of the model: everything here stops before an API call.
func TestRun(t *testing.T) {
	root := docsRoot(t)
	type args struct{ argv []string }
	type want struct {
		Code   int
		Output string
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{name: "-h prints usage and succeeds", args: args{argv: []string{"-h"}}, want: want{Code: exitSuccess}},
		{name: "an unknown flag is a usage error", args: args{argv: []string{"-nope", "q"}}, want: want{Code: exitUsage}},
		{
			name: "a blank question is refused before any documentation is read",
			args: args{argv: []string{"-root", filepath.Join(root, "missing")}},
			want: want{Code: exitUsage},
		},
		{
			name: "a root that does not exist means no input",
			args: args{argv: []string{"-root", filepath.Join(root, "missing"), "outbox"}},
			want: want{Code: exitNoInput},
		},
		{
			name: "a question nothing matches means no input, and no model is called",
			args: args{argv: []string{"-root", root, "-retrieve-only", "how to bake a cake"}},
			want: want{Code: exitNoInput},
		},
		{
			name: "-retrieve-only prints the evidence, best first, without a model",
			args: args{argv: []string{"-root", root, "-retrieve-only", "outbox backlog"}},
			want: want{Code: exitSuccess, Output: "- docs/runbook.md — Kafka down (36 bytes)\n" +
				"- README.md — Kafka-lab (27 bytes)\n" +
				"- experiments/exp-13-outage/README.md — exp-13 (33 bytes)\n" +
				"3 sections, 96 bytes of text would be sent\n"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out strings.Builder

			code := run(slog.New(slog.DiscardHandler), tt.args.argv, &out)

			if diff := cmp.Diff(tt.want, want{Code: code, Output: out.String()}); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
