package outbox

import "time"

// Record is one row of the outbox as the relay sees it: the event is already serialised,
// because the transaction that wrote it could not afford a call to a schema registry.
type Record struct {
	ID          int64
	AggregateID string
	EventType   string
	Payload     []byte
	OccurredAt  time.Time
}
