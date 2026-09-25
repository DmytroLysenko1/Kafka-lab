package kafka

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	paymentv1 "github.com/DmytroLysenko1/Kafka-lab/gen/payment/v1"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
)

var ErrDecode = errors.New("kafka: decode record")

// decodeEvent reads the record the way the wire format says to: the schema id and the
// message indexes come off the front, and what remains is the protobuf. The event id comes
// from the header the relay set — the outbox row id, which survives a republished record
// and is therefore what deduplication has to key on.
func decodeEvent(record *kgo.Record) (merchants.AuthorizedEvent, error) {
	header := sr.ConfluentHeader{}

	id, body, err := header.DecodeID(record.Value)
	if err != nil {
		return merchants.AuthorizedEvent{}, fmt.Errorf("%w: schema id: %w", ErrDecode, err)
	}
	_, body, err = header.DecodeIndex(body, 1)
	if err != nil {
		return merchants.AuthorizedEvent{}, fmt.Errorf("%w: schema %d message index: %w", ErrDecode, id, err)
	}

	var authorized paymentv1.PaymentAuthorized
	if err := proto.Unmarshal(body, &authorized); err != nil {
		return merchants.AuthorizedEvent{}, fmt.Errorf("%w: schema %d: %w", ErrDecode, id, err)
	}

	return merchants.AuthorizedEvent{
		EventID:     headerValue(record.Headers, "event_id"),
		MerchantID:  authorized.GetMerchantId(),
		AmountMinor: authorized.GetAmountMinor(),
	}, nil
}
