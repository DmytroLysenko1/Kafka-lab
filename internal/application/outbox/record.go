package outbox

import "time"

// Record is one row of the outbox as the relay sees it: the event is already serialised,
// because the transaction that wrote it could not afford a call to a schema registry. The
// relay never reads the payload; the format is the adapters' (infrastructure/outboxwire).
type Record struct {
	ID          int64
	AggregateID string
	EventType   string
	Payload     []byte
	OccurredAt  time.Time
	// TraceContext is the trace the event was raised in, opaque to the relay: it is carried
	// from the row to the record so the record stays in the trace of the request behind it.
	TraceContext map[string]string
}
