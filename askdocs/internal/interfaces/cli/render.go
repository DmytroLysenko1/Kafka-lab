// Package cli is what a person at a terminal reads: an answer with the sections it rests on,
// or the evidence a question would be answered from.
package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
)

const ungrounded = "(no section of the documentation was cited: treat this as unsupported)"

// WriteAnswer prints the answer, then each cited section with what the answer quotes from it,
// so the reader can check a claim against the sentence it rests on.
func WriteAnswer(out io.Writer, answer docs.Answer) error {
	var page strings.Builder
	page.WriteString(answer.Text())
	page.WriteString("\n\n")
	if !answer.Grounded() {
		page.WriteString(ungrounded + "\n")
	}
	for _, section := range answer.CitedSections() {
		fmt.Fprintf(&page, "- %s\n", section.Place())
		for _, quote := range answer.QuotesFrom(section) {
			fmt.Fprintf(&page, "    > %s\n", strings.Join(strings.Fields(quote), " "))
		}
	}
	_, err := io.WriteString(out, page.String())
	return err
}

// WriteEvidence prints the sections a question would be answered from and how much text they
// would add to the request.
func WriteEvidence(out io.Writer, evidence docs.Evidence) error {
	var page strings.Builder
	sections := evidence.Sections()
	for _, section := range sections {
		fmt.Fprintf(&page, "- %s (%d bytes)\n", section.Place(), section.Size())
	}
	fmt.Fprintf(&page, "%d sections, %d bytes of text would be sent\n", len(sections), evidence.Size())
	_, err := io.WriteString(out, page.String())
	return err
}
