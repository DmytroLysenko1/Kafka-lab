package main

// tally is what the database says after a run: how many payments were produced, how many
// rows the consumer left behind, and how many distinct payments those rows cover.
type tally struct {
	Produced int
	Rows     int
	Distinct int
}

// Lost counts payments that were produced and never landed in the database. It is not
// Produced-Rows: a run can lose some payments and duplicate others at the same time, and
// subtracting totals would silently net them off against each other.
func (t tally) Lost() int {
	return max(t.Produced-t.Distinct, 0)
}

// Duplicated counts rows beyond the first for a payment.
func (t tally) Duplicated() int {
	return max(t.Rows-t.Distinct, 0)
}

// outcome is what a run actually demonstrated, named in the terms the semantics are named
// in — which is the only way to see that a run demonstrated the wrong one.
type outcome string

const (
	outcomeLost      outcome = "payments lost"
	outcomeDuplicate outcome = "payments duplicated"
	outcomeBoth      outcome = "payments lost AND duplicated"
	outcomeExactly   outcome = "every payment exactly once"
	outcomeUnread    outcome = "nothing was consumed"
)

func (t tally) outcome() outcome {
	switch {
	case t.Rows == 0:
		return outcomeUnread
	case t.Lost() > 0 && t.Duplicated() > 0:
		return outcomeBoth
	case t.Lost() > 0:
		return outcomeLost
	case t.Duplicated() > 0:
		return outcomeDuplicate
	default:
		return outcomeExactly
	}
}

// expected is what each semantics is supposed to demonstrate when the consumer dies in the
// middle. A run that produces anything else has not proved its point, whatever the totals
// look like, and the report says so rather than printing numbers and leaving the reader to
// notice.
var expected = map[string]outcome{
	modeAtMostOnce:  outcomeLost,
	modeAtLeastOnce: outcomeDuplicate,
	modeInbox:       outcomeExactly,
}
