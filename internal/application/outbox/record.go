package outbox

import "time"

const EventTypeAuthorized = "payment.authorized"

// Record is one row of the outbox as the relay sees it: the event is already serialised,
// because the transaction that wrote it could not afford a call to a schema registry.
type Record struct {
	ID          int64
	AggregateID string
	EventType   string
	Payload     []byte
	OccurredAt  time.Time
}

// AuthorizedPayload is the outbox's own format, not the broker's. The store that writes a
// row and the relay that publishes it are two adapters that must agree on it, so it is
// declared once, here, beside the contract they share — and it is deliberately not the
// protobuf message: deciding the wire format needs the schema registry, and no transaction
// should be held open across a call to it.
type AuthorizedPayload struct {
	PaymentID   string    `json:"payment_id"`
	MerchantID  string    `json:"merchant_id"`
	AmountMinor int64     `json:"amount_minor"`
	Currency    string    `json:"currency"`
	OccurredAt  time.Time `json:"occurred_at"`
}
