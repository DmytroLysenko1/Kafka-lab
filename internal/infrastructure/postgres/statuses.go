package postgres

import (
	"errors"
	"fmt"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

var errStatusUnknown = errors.New("postgres: not a status a payment can be stored in")

// storedStatuses is how each status a payment can be in is spelled in the payments table; the
// table's CHECK holds the column to the same set (migrations/00004_capture_refund.sql).
func storedStatuses() map[payment.Status]string {
	return map[payment.Status]string{
		payment.StatusAuthorized: "authorized",
		payment.StatusCaptured:   "captured",
		payment.StatusRefunded:   "refunded",
	}
}

// storedStatus refuses to spell StatusUnknown: writing it would be a bug, and the CHECK would
// refuse it anyway — as a storage failure a retry repeats, where this is a corrupt payment.
func storedStatus(status payment.Status) (string, error) {
	spelling, known := storedStatuses()[status]
	if !known {
		return "", fmt.Errorf("%w: %s", errStatusUnknown, status)
	}
	return spelling, nil
}

// parseStoredStatus reads a status back. A spelling no payment can be in — one a newer version
// wrote, say — is a row this version cannot rebuild, not a payment in an unknown state.
func parseStoredStatus(spelling string) (payment.Status, error) {
	for status, stored := range storedStatuses() {
		if stored == spelling {
			return status, nil
		}
	}
	return payment.StatusUnknown, fmt.Errorf("%w: %q", errStatusUnknown, spelling)
}
