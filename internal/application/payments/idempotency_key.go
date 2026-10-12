package payments

import (
	"errors"
	"strings"
	"unicode/utf8"
)

var (
	ErrIdempotencyKeyRequired = errors.New("payments: idempotency key is required")
	ErrIdempotencyKeyTooLong  = errors.New("payments: idempotency key is longer than allowed")
	ErrIdempotencyKeyInvalid  = errors.New("payments: idempotency key is not UTF-8 text")
)

// MaxIdempotencyKeyLength is Stripe's limit for the same header. The key is stored in a
// unique index, and a key of a few kilobytes — headers may be up to a megabyte — would fail
// there as a storage failure instead of being refused as the caller's mistake.
const MaxIdempotencyKeyLength = 255

// IdempotencyKey is the caller's name for one payment request. It belongs to the caller's
// protocol, not to the payment, so it lives beside the use case rather than in the domain;
// and it can only be made by ParseIdempotencyKey, so a key the unique index could not hold
// never reaches the store — where its refusal would read as an outage and fail on every retry.
type IdempotencyKey struct {
	value string
}

func ParseIdempotencyKey(raw string) (IdempotencyKey, error) {
	switch {
	case raw == "":
		return IdempotencyKey{}, ErrIdempotencyKeyRequired
	case len(raw) > MaxIdempotencyKeyLength:
		return IdempotencyKey{}, ErrIdempotencyKeyTooLong
	case !utf8.ValidString(raw) || strings.ContainsRune(raw, 0):
		return IdempotencyKey{}, ErrIdempotencyKeyInvalid
	}
	return IdempotencyKey{
		value: raw,
	}, nil
}

// IsZero is a command built without parsing its key: every keyless request of a merchant would
// otherwise share the empty key and replay the first one's payment.
func (k IdempotencyKey) IsZero() bool {
	return k.value == ""
}

func (k IdempotencyKey) String() string {
	return k.value
}
