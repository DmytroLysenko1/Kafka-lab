package docs

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// MaxQuestionRunes bounds what one question may cost: a question is a sentence or two, and
// anything longer is a pasted document, which this tool does not answer from.
const MaxQuestionRunes = 2000

var (
	ErrEmptyQuestion   = errors.New("docs: question is empty")
	ErrQuestionTooLong = errors.New("docs: question is longer than a question")
)

type Question struct {
	text string
}

func NewQuestion(text string) (Question, error) {
	text = strings.TrimSpace(text)
	switch {
	case text == "":
		return Question{}, ErrEmptyQuestion
	case utf8.RuneCountInString(text) > MaxQuestionRunes:
		return Question{}, ErrQuestionTooLong
	}
	return Question{
		text: text,
	}, nil
}

func (q Question) String() string {
	return q.text
}
