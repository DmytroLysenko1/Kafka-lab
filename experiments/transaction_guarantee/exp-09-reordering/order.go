package main

// event is one step of one payment. Seq is the order the producer handed events to the
// client, across all payments, so any record arriving with a lower seq than one already
// seen arrived out of order — whatever payment it belongs to.
type event struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
	Seq       int    `json:"seq"`
}

const (
	statusCaptured = "captured"
	statusRefunded = "refunded"
)

// order is what the log says about the order the cluster kept.
type order struct {
	Produced   int
	Read       int
	Distinct   int
	Duplicates int
	// Inversions counts first arrivals of a seq lower than one already seen. A duplicate is
	// not an inversion: a batch sent twice has already been counted once, and counting its
	// second copy again would double-count one failure as two.
	Inversions int
	// RefundedFirst is the business consequence: a payment whose refund was recorded before
	// the capture it refunds.
	RefundedFirst int
	Missing       int
}

func (o order) held() bool {
	return o.Duplicates == 0 && o.Inversions == 0 &&
		o.RefundedFirst == 0 && o.Missing == 0
}

// analyse reads the partition in the order it was delivered and reports what the producer
// failed to keep. It is the whole claim of the experiment, which is why it lives apart
// from the client and is tested on its own.
func analyse(delivered []event, produced int) order {
	seen := make(map[int]bool, len(delivered))
	captured := make(map[string]bool, len(delivered)/2)
	refundedFirst := make(map[string]bool)

	result := order{
		Produced: produced,
		Read:     len(delivered),
	}
	highest := -1
	for _, e := range delivered {
		if seen[e.Seq] {
			result.Duplicates++
			continue
		}
		seen[e.Seq] = true

		if e.Seq < highest {
			result.Inversions++
		}
		highest = max(highest, e.Seq)

		switch e.Status {
		case statusCaptured:
			captured[e.PaymentID] = true
		case statusRefunded:
			if !captured[e.PaymentID] {
				refundedFirst[e.PaymentID] = true
			}
		}
	}

	result.Distinct = len(seen)
	result.RefundedFirst = len(refundedFirst)
	result.Missing = max(produced-result.Distinct, 0)
	return result
}
