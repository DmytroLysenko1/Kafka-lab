package cli_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/interfaces/cli"
)

func evidence(t *testing.T) docs.Evidence {
	t.Helper()
	raw := [][3]string{
		{"docs/runbook.md", "Kafka down", "Wait for the backlog to drain."},
		{"README.md", "", "A lab for Kafka."},
	}
	sections := lo.Map(raw, func(r [3]string, _ int) docs.Section {
		section, err := docs.NewSection(r[0], r[1], r[2])
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		return section
	})
	found, err := docs.NewEvidence(sections)
	if err != nil {
		t.Fatalf("evidence: %v", err)
	}
	return found
}

func TestWriteAnswer(t *testing.T) {
	type args struct{ cited []int }
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr error
	}{
		{
			name: "each cited section is listed under the answer with what it is quoted for",
			args: args{cited: []int{0, 1, 0}},
			want: "The backlog drains.\n\n" +
				"- docs/runbook.md — Kafka down\n    > quote 0\n    > quote 2\n" +
				"- README.md\n    > quote 1\n",
		},
		{
			name: "an answer that cites nothing is marked as unsupported",
			args: args{},
			want: "The backlog drains.\n\n(no section of the documentation was cited: treat this as unsupported)\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			found := evidence(t)
			cites := lo.Map(tt.args.cited, func(position, i int) docs.Cite {
				return docs.Cite{Position: position, Quote: "quote " + string(rune('0'+i))}
			})
			answer, err := found.Answer("The backlog drains.", cites)
			if err != nil {
				t.Fatalf("answer: %v", err)
			}
			var page strings.Builder

			err = cli.WriteAnswer(&page, answer)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, page.String()); diff != "" {
				t.Errorf("page mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWriteEvidence(t *testing.T) {
	var page strings.Builder

	if err := cli.WriteEvidence(&page, evidence(t)); err != nil {
		t.Fatalf("err = %v", err)
	}

	want := "- docs/runbook.md — Kafka down (30 bytes)\n" +
		"- README.md (16 bytes)\n" +
		"2 sections, 46 bytes of text would be sent\n"
	if diff := cmp.Diff(want, page.String()); diff != "" {
		t.Errorf("page mismatch (-want +got):\n%s", diff)
	}
}
