package docs

import (
	"errors"
	"strings"

	"github.com/samber/lo"
)

// Why no answer arrived. ErrModelUnavailable is worth asking again later and
// ErrRequestRejected is not; the rest say the answer itself is missing or unusable.
var (
	ErrModelUnavailable = errors.New("docs: the model did not answer within the retry budget")
	ErrRequestRejected  = errors.New("docs: the model rejected the request")
	ErrModelFailed      = errors.New("docs: the call to the model failed in a way nothing here classifies")
	ErrRefused          = errors.New("docs: the model declined to answer")
	ErrTruncated        = errors.New("docs: the answer stopped before it was finished")
	ErrEmptyAnswer      = errors.New("docs: the model answered with no text")
)

// Cite is what a model says about one quote: which section of the evidence, by its position,
// and the text it quotes.
type Cite struct {
	Position int
	Quote    string
}

// Answer is the model's text with the parts of the evidence it quotes. It is made only by
// Evidence.Answer, so every citation in it points at a section the model was given.
type Answer struct {
	text      string
	citations []citation
}

type citation struct {
	section Section
	quote   string
}

func (a Answer) Text() string {
	return a.text
}

// Grounded says whether any of the answer rests on the evidence. An answer that cites nothing
// is the model speaking for itself, or saying the docs do not cover the question, and the
// reader is told so instead of being handed it as the docs' word.
func (a Answer) Grounded() bool {
	return len(a.citations) > 0
}

// CitedSections lists each section the answer cites once, in the order it first cites them.
func (a Answer) CitedSections() []Section {
	return lo.Uniq(lo.Map(a.citations, func(c citation, _ int) Section { return c.section }))
}

// QuotesFrom is what the answer quotes from one section, in the order it quotes it.
func (a Answer) QuotesFrom(section Section) []string {
	return lo.FilterMap(a.citations, func(c citation, _ int) (string, bool) {
		return c.quote, c.section == section
	})
}

func newAnswer(text string, citations []citation) (Answer, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return Answer{}, ErrEmptyAnswer
	}
	return Answer{
		text:      text,
		citations: citations,
	}, nil
}
