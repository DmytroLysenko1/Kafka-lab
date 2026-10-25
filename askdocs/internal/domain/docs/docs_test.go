package docs_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
)

func TestNewQuestion(t *testing.T) {
	type args struct{ text string }
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr error
	}{
		{name: "surrounding space is not part of the question", args: args{text: "  why acks=all?\n"}, want: "why acks=all?"},
		{name: "blank is no question", args: args{text: " \t\n"}, wantErr: docs.ErrEmptyQuestion},
		{name: "exactly at the limit is a question", args: args{text: strings.Repeat("я", docs.MaxQuestionRunes)}, want: strings.Repeat("я", docs.MaxQuestionRunes)},
		{name: "one rune past the limit is a pasted document", args: args{text: strings.Repeat("я", docs.MaxQuestionRunes+1)}, wantErr: docs.ErrQuestionTooLong},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := docs.NewQuestion(tt.args.text)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got.String()); diff != "" {
				t.Errorf("question mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewSection(t *testing.T) {
	type args struct{ document, heading, body string }
	type want struct {
		Title string
		Place string
		Body  string
		Size  int
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "a section is called by its heading, and its text is trimmed",
			args: args{document: "docs/runbook.md", heading: " Roll back ", body: "\nhelm rollback.\n"},
			want: want{Title: "Roll back", Place: "docs/runbook.md — Roll back", Body: "helm rollback.", Size: len("helm rollback.")},
		},
		{
			name: "text before a document's first heading is called by the document",
			args: args{document: "README.md", body: "A lab."},
			want: want{Title: "README.md", Place: "README.md", Body: "A lab.", Size: len("A lab.")},
		},
		{name: "a heading with nothing under it is not a section", args: args{document: "docs/runbook.md", heading: "Empty", body: " \n\n "}, wantErr: docs.ErrEmptySection},
		{name: "text with no document is not citable", args: args{document: " ", body: "text"}, wantErr: docs.ErrEmptySection},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			section, err := docs.NewSection(tt.args.document, tt.args.heading, tt.args.body)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			got := want{Title: section.Title(), Place: section.Place(), Body: section.Body(), Size: section.Size()}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("section mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func sections(t *testing.T, count int) []docs.Section {
	t.Helper()
	return lo.Times(count, func(i int) docs.Section {
		section, err := docs.NewSection("docs/case.md", string(rune('A'+i)), strings.Repeat("x", i+1))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		return section
	})
}

func headings(sections []docs.Section) []string {
	return lo.Map(sections, func(section docs.Section, _ int) string { return section.Heading() })
}

func TestNewEvidence(t *testing.T) {
	type args struct{ ranked int }
	type want struct {
		Headings []string
		Size     int
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{name: "nothing ranked is no evidence: the model is not asked to guess", args: args{ranked: 0}, wantErr: docs.ErrNothingRelevant},
		{name: "fewer sections than the cap are all kept, best first", args: args{ranked: 2}, want: want{Headings: []string{"A", "B"}, Size: 1 + 2}},
		{
			name: "past the cap only the best sections are kept",
			args: args{ranked: docs.MaxEvidence + 3},
			want: want{Headings: []string{"A", "B", "C", "D", "E", "F"}, Size: 1 + 2 + 3 + 4 + 5 + 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evidence, err := docs.NewEvidence(sections(t, tt.args.ranked))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			got := want{Headings: headings(evidence.Sections()), Size: evidence.Size()}
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("evidence mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEvidenceAnswer(t *testing.T) {
	type args struct {
		text  string
		cites []docs.Cite
	}
	type want struct {
		Text     string
		Grounded bool
		Cited    []string
		QuotesA  []string
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "a section cited twice is listed once, with both quotes, in first-cited order",
			args: args{text: " answer ", cites: []docs.Cite{{Position: 0, Quote: "first"}, {Position: 1, Quote: "other"}, {Position: 0, Quote: "second"}}},
			want: want{Text: "answer", Grounded: true, Cited: []string{"A", "B"}, QuotesA: []string{"first", "second"}},
		},
		{
			name: "a citation of a position the model was never given is dropped",
			args: args{text: "answer", cites: []docs.Cite{{Position: 2, Quote: "invented"}, {Position: -1, Quote: "invented"}}},
			want: want{Text: "answer"},
		},
		{name: "an answer that cites nothing is not grounded", args: args{text: "answer"}, want: want{Text: "answer"}},
		{name: "an answer with no text is no answer", args: args{text: " \n"}, wantErr: docs.ErrEmptyAnswer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evidence, err := docs.NewEvidence(sections(t, 2))
			if err != nil {
				t.Fatalf("evidence: %v", err)
			}

			answer, err := evidence.Answer(tt.args.text, tt.args.cites)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			got := want{
				Text:     answer.Text(),
				Grounded: answer.Grounded(),
				Cited:    headings(answer.CitedSections()),
				QuotesA:  answer.QuotesFrom(evidence.Sections()[0]),
			}
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("answer mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The zero Answer, the only one made outside Evidence.Answer, cites nothing.
func TestZeroAnswerIsNotGrounded(t *testing.T) {
	var answer docs.Answer
	got := []any{answer.Grounded(), len(answer.CitedSections())}
	if diff := cmp.Diff([]any{false, 0}, got); diff != "" {
		t.Errorf("zero answer mismatch (-want +got):\n%s", diff)
	}
}
