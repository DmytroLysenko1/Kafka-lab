package postgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/outboxwire"
)

var errUnknownEvent = errors.New("postgres: no outbox mapping for the event")

type outboxRow struct {
	aggregateID string
	eventType   string
	payload     []byte
	occurredAt  time.Time
}

// eventType is the one place a domain event is given its name on the wire. The switch is
// exhaustive because Event is sealed: a new event fails here, loudly, until it is named.
func eventType(event payment.Event) (string, error) {
	switch event.(type) {
	case *payment.Authorized:
		return outboxwire.EventTypeAuthorized, nil
	case *payment.Captured:
		return outboxwire.EventTypeCaptured, nil
	case *payment.Refunded:
		return outboxwire.EventTypeRefunded, nil
	}
	return "", fmt.Errorf("postgres.eventType %T: %w", event, errUnknownEvent)
}

func encodeEvent(event payment.Event) (outboxRow, error) {
	name, err := eventType(event)
	if err != nil {
		return outboxRow{}, err
	}
	payload, err := json.Marshal(outboxwire.Payload{
		PaymentID:   event.PaymentID().String(),
		MerchantID:  event.Merchant().String(),
		AmountMinor: event.Amount().Minor(),
		Currency:    event.Amount().Currency().String(),
		OccurredAt:  event.OccurredAt(),
	})
	if err != nil {
		return outboxRow{}, fmt.Errorf("postgres.encodeEvent %s: %w", name, err)
	}
	return outboxRow{
		aggregateID: event.PaymentID().String(),
		eventType:   name,
		payload:     payload,
		occurredAt:  event.OccurredAt(),
	}, nil
}
