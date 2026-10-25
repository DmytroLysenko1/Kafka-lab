package ask_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/application/ask"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/infrastructure/bm25"
)

var errRateLimited = errors.New("429 after every retry")

// citingModel stands in for Claude the way the real one behaves on a good answer: it cites
// every section of the evidence it was given. It records the questions it was paid for.
type citingModel struct {
	err   error
	asked []string
}

func (m *citingModel) Answer(_ context.Context, question docs.Question, evidence docs.Evidence) (docs.Answer, error) {
	m.asked = append(m.asked, question.String())
	if m.err != nil {
		return docs.Answer{}, m.err
	}
	cites := lo.Map(evidence.Sections(), func(section docs.Section, i int) docs.Cite {
		return docs.Cite{Position: i, Quote: section.Body()}
	})
	return evidence.Answer("answer", cites)
}

func documentation(t *testing.T, matching int) []docs.Section {
	t.Helper()
	sections := lo.Times(matching, func(i int) docs.Section {
		return section(t, "docs/outbox.md", string(rune('A'+i)), "the outbox relay publishes to kafka")
	})
	return append(sections, section(t, "docs/k8s.md", "Helm", "helm rollback restores a revision"))
}

func section(t *testing.T, document, heading, body string) docs.Section {
	t.Helper()
	s, err := docs.NewSection(document, heading, body)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return s
}

func question(t *testing.T, text string) docs.Question {
	t.Helper()
	q, err := docs.NewQuestion(text)
	if err != nil {
		t.Fatalf("question: %v", err)
	}
	return q
}

func TestExecute(t *testing.T) {
	type args struct {
		question string
		matching int
		modelErr error
	}
	type want struct {
		Cited int
		Asked []string
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "the model reads only matching sections, capped at the evidence limit",
			args: args{question: "how does the outbox relay publish?", matching: docs.MaxEvidence + 3},
			want: want{Cited: docs.MaxEvidence, Asked: []string{"how does the outbox relay publish?"}},
		},
		{
			name: "fewer matches than the limit are all sent",
			args: args{question: "outbox", matching: 2},
			want: want{Cited: 2, Asked: []string{"outbox"}},
		},
		{
			name:    "a question nothing matches is not paid for",
			args:    args{question: "how to bake a cake", matching: 3},
			wantErr: docs.ErrNothingRelevant,
		},
		{
			name:    "the model's failure reaches the caller unchanged in kind",
			args:    args{question: "outbox", matching: 1, modelErr: errors.Join(docs.ErrModelUnavailable, errRateLimited)},
			want:    want{Asked: []string{"outbox"}},
			wantErr: docs.ErrModelUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &citingModel{err: tt.args.modelErr}
			questions := ask.New(bm25.New(documentation(t, tt.args.matching)), model, noop.NewTracerProvider().Tracer(""))

			answer, err := questions.Execute(t.Context(), question(t, tt.args.question))

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			got := want{Cited: len(answer.CitedSections()), Asked: model.asked}
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEvidenceNeverCallsTheModel(t *testing.T) {
	model := &citingModel{}
	questions := ask.New(bm25.New(documentation(t, 2)), model, noop.NewTracerProvider().Tracer(""))

	evidence, err := questions.Evidence(t.Context(), question(t, "outbox relay"))

	if err != nil {
		t.Fatalf("err = %v", err)
	}
	got := lo.Map(evidence.Sections(), func(s docs.Section, _ int) string { return s.Document() + "#" + s.Heading() })
	if diff := cmp.Diff([]string{"docs/outbox.md#A", "docs/outbox.md#B"}, got); diff != "" {
		t.Errorf("evidence mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string(nil), model.asked); diff != "" {
		t.Errorf("retrieval alone asked the model (-want +got):\n%s", diff)
	}
}
