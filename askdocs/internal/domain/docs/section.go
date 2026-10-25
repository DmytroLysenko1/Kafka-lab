// Package docs is the language of asking the lab's documentation a question: the sections
// that can be cited, the evidence an answer may rest on, and an answer that says which of it
// it used.
package docs

import (
	"cmp"
	"errors"
	"strings"
)

var ErrEmptySection = errors.New("docs: section has no document or no text")

// Section is one heading of one document and the text under it: the unit that is retrieved,
// handed to the model and cited back. A heading with no text under it is not one, so it never
// costs a token.
type Section struct {
	document string
	heading  string
	body     string
}

func NewSection(document, heading, body string) (Section, error) {
	document, body = strings.TrimSpace(document), strings.TrimSpace(body)
	if document == "" || body == "" {
		return Section{}, ErrEmptySection
	}
	return Section{
		document: document,
		heading:  strings.TrimSpace(heading),
		body:     body,
	}, nil
}

// Document is the file the section belongs to, as a path from the repository root.
func (s Section) Document() string {
	return s.document
}

func (s Section) Heading() string {
	return s.heading
}

func (s Section) Body() string {
	return s.body
}

// Title is what a reader calls the section: its heading, or the document for the text that
// comes before a document's first heading.
func (s Section) Title() string {
	return cmp.Or(s.heading, s.document)
}

// Place is where a reader finds the section: the document, and the heading when it has one.
func (s Section) Place() string {
	if s.heading == "" {
		return s.document
	}
	return s.document + " — " + s.heading
}

// Size is the section's text in bytes, the part of a request that grows with the evidence.
func (s Section) Size() int {
	return len(s.body)
}
