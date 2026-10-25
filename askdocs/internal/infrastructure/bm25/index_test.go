package bm25_test

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/infrastructure/bm25"
)

func corpus(t *testing.T) []docs.Section {
	t.Helper()
	raw := []struct{ source, heading, body string }{
		{"docs/static/06-transactional-outbox.md", "What the relay must get right", "The relay publishes outbox rows to Kafka and marks them published only after the broker acknowledges."},
		{"experiments/exp-13-broker-outage/README.md", "Result", "With every broker stopped the API kept accepting payments; the outbox backlog grew and drained after recovery."},
		{"docs/static/07-retry-dlq.md", "Case B", "A message that keeps failing is retried through delay topics and then parked in the dead letter topic."},
		{"docs/k8s/runbook.md", "Roll back", "helm rollback returns the release to the previous revision; the database migration is not undone."},
		{"docs/00-journal.md", "Notes", "Kafka Kafka Kafka Kafka Kafka Kafka Kafka Kafka Kafka Kafka Kafka Kafka relay."},
	}
	return lo.Map(raw, func(r struct{ source, heading, body string }, _ int) docs.Section {
		section, err := docs.NewSection(r.source, r.heading, r.body)
		if err != nil {
			t.Fatalf("fixture %s: %v", r.source, err)
		}
		return section
	})
}

func TestRelevant(t *testing.T) {
	type args struct {
		question string
		limit    int
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr error
	}{
		{
			name: "the section the question names comes first",
			args: args{question: "What happens to payments during a broker outage?", limit: 1},
			want: []string{"experiments/exp-13-broker-outage/README.md"},
		},
		{
			name: "inflected forms of a word match each other",
			args: args{question: "how are retries parked?", limit: 1},
			want: []string{"docs/static/07-retry-dlq.md"},
		},
		{
			name: "a section with every term outranks one that repeats a single term",
			args: args{question: "kafka relay acknowledges", limit: 1},
			want: []string{"docs/static/06-transactional-outbox.md"},
		},
		{
			name: "the limit caps the answer",
			args: args{question: "outbox relay broker", limit: 2},
			want: []string{"docs/static/06-transactional-outbox.md", "experiments/exp-13-broker-outage/README.md"},
		},
		{
			name: "nothing in common returns nothing, not the least bad section",
			args: args{question: "how to bake a cake", limit: 3},
			want: nil,
		},
		{
			name: "a limit of zero asks for nothing and gets nothing",
			args: args{question: "outbox relay broker", limit: 0},
			want: nil,
		},
		{
			name: "a negative limit is treated as zero, not as a crash",
			args: args{question: "outbox relay broker", limit: -1},
			want: nil,
		},
		{
			name: "a question made only of stop words matches nothing",
			args: args{question: "what is it and how", limit: 3},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := bm25.New(corpus(t))
			question, err := docs.NewQuestion(tt.args.question)
			if err != nil {
				t.Fatalf("question: %v", err)
			}
			sections, err := index.Relevant(t.Context(), question, tt.args.limit)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			got := lo.Map(sections, func(section docs.Section, _ int) string { return section.Document() })
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ranking mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A heading names what its body is about, so a section headed by the question's topic ranks
// above one that only mentions it in passing. The headed section's body is the longer one, so
// without the heading's weight, length normalisation would rank it second.
func TestRelevantWeighsTheHeading(t *testing.T) {
	mention, err := docs.NewSection("docs/a.md", "Consumer offsets", "Offsets are committed after the rebalance settles.")
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	headed, err := docs.NewSection("docs/b.md", "Rebalance", "Partitions move between the members of the group whenever one of them joins, leaves or stalls.")
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	question, err := docs.NewQuestion("rebalance")
	if err != nil {
		t.Fatalf("question: %v", err)
	}

	got, err := bm25.New([]docs.Section{mention, headed}).Relevant(t.Context(), question, 2)

	if err != nil {
		t.Fatalf("err = %v", err)
	}
	documents := lo.Map(got, func(section docs.Section, _ int) string { return section.Document() })
	if diff := cmp.Diff([]string{"docs/b.md", "docs/a.md"}, documents); diff != "" {
		t.Errorf("ranking mismatch (-want +got):\n%s", diff)
	}
}

func TestRelevantOnEmptyIndex(t *testing.T) {
	question, err := docs.NewQuestion("kafka")
	if err != nil {
		t.Fatalf("question: %v", err)
	}
	got, err := bm25.New(nil).Relevant(t.Context(), question, 3)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if diff := cmp.Diff([]docs.Section{}, got, cmpopts.EquateEmpty(), cmp.AllowUnexported(docs.Section{})); diff != "" {
		t.Errorf("an empty index returned sections (-want +got):\n%s", diff)
	}
}
