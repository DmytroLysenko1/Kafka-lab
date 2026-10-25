package docs

import (
	"errors"
	"slices"

	"github.com/samber/lo"
)

// MaxEvidence is how many sections one question may be answered from: a chosen cap, not a
// measured optimum. Every section is input paid on every question; what six cost is in
// results/retrieval.log.
const MaxEvidence = 6

var ErrNothingRelevant = errors.New("docs: no section of the documentation matches the question")

// Evidence is the sections a question will be answered from, best first. It is never empty:
// with nothing to read, a model could only guess, so there is no evidence to hand it.
type Evidence struct {
	sections []Section
}

func NewEvidence(ranked []Section) (Evidence, error) {
	if len(ranked) == 0 {
		return Evidence{}, ErrNothingRelevant
	}
	return Evidence{
		sections: slices.Clone(ranked[:min(MaxEvidence, len(ranked))]),
	}, nil
}

func (e Evidence) Sections() []Section {
	return slices.Clone(e.sections)
}

func (e Evidence) Size() int {
	return lo.SumBy(e.sections, Section.Size)
}

// Answer binds the model's text to this evidence. An answer can only cite what it was given:
// a position outside the evidence names no section, and that citation is dropped.
func (e Evidence) Answer(text string, cites []Cite) (Answer, error) {
	citations := lo.FilterMap(cites, func(cite Cite, _ int) (citation, bool) {
		if cite.Position < 0 || cite.Position >= len(e.sections) {
			return citation{}, false
		}
		return citation{section: e.sections[cite.Position], quote: cite.Quote}, true
	})
	return newAnswer(text, citations)
}
