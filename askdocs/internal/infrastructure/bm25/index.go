// Package bm25 retrieves documentation sections by Okapi BM25, in memory. It is the
// retrieval step of the RAG pipeline without an embeddings provider: the corpus is a few
// hundred sections of English prose with a stable vocabulary (ISR, outbox, rebalance), where
// a word match finds what a question names.
package bm25

import (
	"cmp"
	"context"
	"math"
	"slices"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
)

// The textbook parameters (Robertson & Zaragoza, "The Probabilistic Relevance Framework",
// 2009): k1 saturates repeated terms, b discounts long sections.
const (
	termSaturation    = 1.2
	lengthNormalising = 0.75
)

// headingRepetitions weights the heading over the body: a heading names what its body is
// about, so a question naming that topic should rank the section above one that mentions it.
const headingRepetitions = 2

type Index struct {
	sections      []docs.Section
	termCounts    []map[string]int
	lengths       []int
	averageLength float64
	sectionsWith  map[string]int
}

func New(sections []docs.Section) *Index {
	index := &Index{
		sections:     slices.Clone(sections),
		termCounts:   make([]map[string]int, len(sections)),
		lengths:      make([]int, len(sections)),
		sectionsWith: make(map[string]int),
	}
	total := 0
	for i, section := range index.sections {
		index.termCounts[i], index.lengths[i] = countTerms(section)
		total += index.lengths[i]
		for term := range index.termCounts[i] {
			index.sectionsWith[term]++
		}
	}
	if len(sections) > 0 {
		index.averageLength = float64(total) / float64(len(sections))
	}
	return index
}

// Relevant returns at most limit sections that share a term with the question, best first.
// A section that shares none scores zero and is never returned: no match means no answer.
func (ix *Index) Relevant(_ context.Context, question docs.Question, limit int) ([]docs.Section, error) {
	terms := tokens(question.String())
	type scored struct {
		section docs.Section
		score   float64
	}
	matches := lo.FilterMap(ix.sections, func(section docs.Section, i int) (scored, bool) {
		score := ix.score(i, terms)
		return scored{
			section: section,
			score:   score,
		}, score > 0
	})
	slices.SortStableFunc(matches, func(a, b scored) int {
		return cmp.Compare(b.score, a.score)
	})
	best := matches[:min(max(limit, 0), len(matches))]
	return lo.Map(best, func(match scored, _ int) docs.Section {
		return match.section
	}), nil
}

func (ix *Index) score(position int, terms []string) float64 {
	score := 0.0
	lengthRatio := float64(ix.lengths[position]) / ix.averageLength
	for _, term := range terms {
		frequency := float64(ix.termCounts[position][term])
		if frequency == 0 {
			continue
		}
		saturated := frequency * (termSaturation + 1) /
			(frequency + termSaturation*(1-lengthNormalising+lengthNormalising*lengthRatio))
		score += ix.rarity(term) * saturated
	}
	return score
}

// rarity is BM25's inverse document frequency, in the form that stays positive for a term
// found in more than half of the sections.
func (ix *Index) rarity(term string) float64 {
	sections, with := float64(len(ix.sections)), float64(ix.sectionsWith[term])
	return math.Log(1 + (sections-with+0.5)/(with+0.5))
}

// countTerms counts the section's document path, heading and body. The path carries the
// experiment's name (exp-13-broker-outage), so it counts as text too.
func countTerms(section docs.Section) (map[string]int, int) {
	counts := make(map[string]int)
	length := 0
	add := func(text string) {
		for _, term := range tokens(text) {
			counts[term]++
			length++
		}
	}
	add(section.Document())
	for range headingRepetitions {
		add(section.Heading())
	}
	add(section.Body())
	return counts, length
}
