package stand

import "fmt"

// Tally is what the database says after a run: how many payments were produced, how many
// rows the consumer left behind, and how many distinct payments those rows cover.
type Tally struct {
	Produced int
	Rows     int
	Distinct int
	// Refused counts redeliveries the inbox turned away because the payment was already
	// claimed. Only exp-07 writes through the inbox, so for the other two it stays 0.
	Refused int
}

// Lost counts payments that were produced and never landed in the database. It is not
// Produced-Rows: a run can lose some payments and duplicate others at the same time, and
// subtracting totals would silently net them off against each other.
func (t Tally) Lost() int {
	return max(t.Produced-t.Distinct, 0)
}

// Duplicated counts rows beyond the first for a payment.
func (t Tally) Duplicated() int {
	return max(t.Rows-t.Distinct, 0)
}

// Outcome is what a run actually demonstrated, named in the terms the semantics are named
// in — which is the only way to see that a run demonstrated the wrong one.
type Outcome string

const (
	OutcomeLost      Outcome = "payments lost"
	OutcomeDuplicate Outcome = "payments duplicated"
	OutcomeBoth      Outcome = "payments lost AND duplicated"
	OutcomeExactly   Outcome = "every payment exactly once"
	OutcomeUnread    Outcome = "nothing was consumed"
)

func (t Tally) Outcome() Outcome {
	switch {
	case t.Rows == 0:
		return OutcomeUnread
	case t.Lost() > 0 && t.Duplicated() > 0:
		return OutcomeBoth
	case t.Lost() > 0:
		return OutcomeLost
	case t.Duplicated() > 0:
		return OutcomeDuplicate
	default:
		return OutcomeExactly
	}
}

// Verdict says whether a run demonstrated what its mode exists to demonstrate. For the
// inbox a clean table is not enough: a run in which nothing was redelivered comes out
// exactly once as well, and proves nothing about deduplication.
func (m Mode) Verdict(t Tally) (string, bool) {
	want := m.Expects()
	switch {
	case t.Outcome() != want:
		return fmt.Sprintf("EXPECTED %q — this run did not demonstrate what it exists to demonstrate", want), false
	case m == Inbox && t.Refused == 0:
		return "EXPECTED the inbox to refuse redeliveries — none arrived, so a clean table proves nothing", false
	default:
		return "as expected", true
	}
}
