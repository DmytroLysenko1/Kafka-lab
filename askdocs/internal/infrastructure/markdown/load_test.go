package markdown_test

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/infrastructure/markdown"
)

type section struct {
	Source, Heading, Body string
}

func TestLoad(t *testing.T) {
	type args struct {
		files fstest.MapFS
		roots []string
	}
	tests := []struct {
		name    string
		args    args
		want    []section
		wantErr error
	}{
		{
			name: "each heading opens a section, and text before the first one is kept",
			args: args{
				files: fstest.MapFS{"docs/runbook.md": file("Intro line.\n# Runbook\nTop.\n## Kafka down\nWait.\n")},
				roots: []string{"docs"},
			},
			want: []section{
				{Source: "docs/runbook.md", Body: "Intro line."},
				{Source: "docs/runbook.md", Heading: "Runbook", Body: "Top."},
				{Source: "docs/runbook.md", Heading: "Kafka down", Body: "Wait."},
			},
		},
		{
			name: "a # line inside a code fence is code, not a heading",
			args: args{
				files: fstest.MapFS{"docs/a.md": file("# Run it\n```sh\n# start the stand\nmake up\n```\nDone.\n")},
				roots: []string{"docs"},
			},
			want: []section{
				{Source: "docs/a.md", Heading: "Run it", Body: "```sh\n# start the stand\nmake up\n```\nDone."},
			},
		},
		{
			name: "a heading with nothing under it adds no section",
			args: args{
				files: fstest.MapFS{"docs/a.md": file("# Title\n\n## Only\nText.\n")},
				roots: []string{"docs"},
			},
			want: []section{{Source: "docs/a.md", Heading: "Only", Body: "Text."}},
		},
		{
			name: "#hashtag is text and only Markdown files are read",
			args: args{
				files: fstest.MapFS{
					"docs/a.md":      file("# T\n#hashtag stays\n"),
					"docs/notes.txt": file("# not markdown\n"),
				},
				roots: []string{"docs"},
			},
			want: []section{{Source: "docs/a.md", Heading: "T", Body: "#hashtag stays"}},
		},
		{
			name: "superseded runs are not the lab's answer and are skipped",
			args: args{
				files: fstest.MapFS{
					"experiments/exp-01/README.md":                    file("# Result\nCurrent.\n"),
					"experiments/exp-01/results/superseded/README.md": file("# Result\nOld.\n"),
				},
				roots: []string{"experiments"},
			},
			want: []section{{Source: "experiments/exp-01/README.md", Heading: "Result", Body: "Current."}},
		},
		{
			name: "a root may be a single file",
			args: args{
				files: fstest.MapFS{"README.md": file("# Kafka-lab\nA lab.\n")},
				roots: []string{"README.md"},
			},
			want: []section{{Source: "README.md", Heading: "Kafka-lab", Body: "A lab."}},
		},
		{
			name:    "a missing root is an error, not an empty corpus",
			args:    args{files: fstest.MapFS{"docs/a.md": file("# T\nx\n")}, roots: []string{"docs", "missing"}},
			wantErr: markdown.ErrRootMissing,
		},
		{
			name:    "roots with no Markdown at all are reported",
			args:    args{files: fstest.MapFS{"docs/a.txt": file("x")}, roots: []string{"docs"}},
			wantErr: markdown.ErrNoDocuments,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sections, err := markdown.Load(t.Context(), tt.args.files, tt.args.roots...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			got := lo.Map(sections, func(s docs.Section, _ int) section {
				return section{Source: s.Document(), Heading: s.Heading(), Body: s.Body()}
			})
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("sections mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadStopsWhenTheCallerGivesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := markdown.Load(ctx, fstest.MapFS{"docs/a.md": file("# T\nx\n")}, "docs")

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want %v", err, context.Canceled)
	}
}

func file(content string) *fstest.MapFile {
	return &fstest.MapFile{Data: []byte(content)}
}
