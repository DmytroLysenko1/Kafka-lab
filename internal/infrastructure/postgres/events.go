package postgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/outbox"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

var errUnknownEvent = errors.New("postgres: no outbox mapping for the event")

type outboxRow struct {
	aggregateID string
	eventType   string
	payload     []byte
	occurredAt  time.Time
}

func encodeEvent(event payment.Event) (outboxRow, error) {
	authorized, isAuthorized := event.(*payment.Authorized)
	if !isAuthorized {
		return outboxRow{}, fmt.Errorf("postgres.encodeEvent %T: %w", event, errUnknownEvent)
	}

	payload, err := json.Marshal(outbox.AuthorizedPayload{
		PaymentID:   authorized.PaymentID().String(),
		MerchantID:  authorized.Merchant().String(),
		AmountMinor: authorized.Amount().Minor(),
		Currency:    authorized.Amount().Currency().String(),
		OccurredAt:  authorized.OccurredAt(),
	})
	if err != nil {
		return outboxRow{}, fmt.Errorf("postgres.encodeEvent %s: %w", outbox.EventTypeAuthorized, err)
	}

	return outboxRow{
		aggregateID: authorized.PaymentID().String(),
		eventType:   outbox.EventTypeAuthorized,
		payload:     payload,
		occurredAt:  authorized.OccurredAt(),
	}, nil
}
