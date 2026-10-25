package bm25

import (
	"strings"
	"unicode"

	"github.com/kljensen/snowball/english"
	"github.com/samber/lo"
)

// tokens lowercases, splits at anything that is not a letter or a digit, drops stop words
// and stems what is left, so "retries", "retried" and "retry" are one term.
func tokens(text string) []string {
	words := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	return lo.FilterMap(words, func(word string, _ int) (string, bool) {
		if len(word) < 2 || english.IsStopWord(word) {
			return "", false
		}
		return english.Stem(word, false), true
	})
}
