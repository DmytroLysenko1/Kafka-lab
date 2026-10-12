// Package outboxwire is the format an outbox row is written in. The store that writes a row
// and the relay that publishes it are two adapters that must agree on it, so it is declared
// once, here, between them — and it is deliberately not the protobuf message: deciding the
// wire format on the topic needs the schema registry, and no transaction should be held open
// across a call to it. The relay itself only moves rows and never reads this format.
package outboxwire

import "time"

const (
	EventTypeAuthorized = "payment.authorized"
	EventTypeCaptured   = "payment.captured"
	EventTypeRefunded   = "payment.refunded"
)

// Payload is what every payment event carries; the row's event type says what happened.
type Payload struct {
	PaymentID   string    `json:"payment_id"`
	MerchantID  string    `json:"merchant_id"`
	AmountMinor int64     `json:"amount_minor"`
	Currency    string    `json:"currency"`
	OccurredAt  time.Time `json:"occurred_at"`
}
