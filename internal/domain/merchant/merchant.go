// Package merchant is the merchants' side of the payments: what a merchant has been
// authorised, as the consumer counts it. Its values arrive over a topic from a producer at
// any version, so every invariant is checked where the value is made, and a value that
// exists is one that can be stored and counted.
package merchant

import "errors"

var (
	ErrIDRequired         = errors.New("merchant: the merchant id is required")
	ErrIDTooLong          = errors.New("merchant: the merchant id is longer than any real one")
	ErrEventIDRequired    = errors.New("merchant: the authorisation carries no event id to deduplicate it by")
	ErrEventIDTooLong     = errors.New("merchant: the event id is longer than any real one")
	ErrAmountNotPositive  = errors.New("merchant: an authorised amount is positive")
	ErrAmountOutOfBalance = errors.New("merchant: the amount does not fit the running total")
)

// maxIdentifierLength matches what the payments domain allows a merchant id to be. Both
// identifiers become primary keys, and a producer at any version — or one that is not ours
// at all — can put a megabyte in a header, and a megabyte-wide key would be stored, indexed
// and kept forever. It is repeated here rather than imported: the payments context issues
// the ids and this one counts them, and neither should change when the other does.
const maxIdentifierLength = 64

// maxAuthorizedMinor is a sanity bound, not a business rule: it leaves room for a total to
// be added to without ever reaching the point where int64 wraps and a merchant's balance
// turns negative on the way past the largest amount Postgres can hold.
const maxAuthorizedMinor = 1 << 50

// ID is a merchant as the projection keys it.
type ID struct {
	value string
}

func ParseID(value string) (ID, error) {
	switch {
	case value == "":
		return ID{}, ErrIDRequired
	case len(value) > maxIdentifierLength:
		return ID{}, ErrIDTooLong
	}
	return ID{
		value: value,
	}, nil
}

func (id ID) String() string {
	return id.value
}

// Authorization is one authorised amount a merchant's total is owed, and the id of the
// event that announced it — the key a redelivery is recognised by.
type Authorization struct {
	eventID  string
	merchant ID
	minor    int64
}

func NewAuthorization(eventID, merchantID string, minor int64) (Authorization, error) {
	if err := checkEventID(eventID); err != nil {
		return Authorization{}, err
	}
	merchant, err := ParseID(merchantID)
	if err != nil {
		return Authorization{}, err
	}
	if err := checkAmount(minor); err != nil {
		return Authorization{}, err
	}
	return Authorization{
		eventID:  eventID,
		merchant: merchant,
		minor:    minor,
	}, nil
}

func (a Authorization) EventID() string {
	return a.eventID
}

func (a Authorization) Merchant() ID {
	return a.merchant
}

func (a Authorization) Minor() int64 {
	return a.minor
}

func checkEventID(eventID string) error {
	switch {
	case eventID == "":
		return ErrEventIDRequired
	case len(eventID) > maxIdentifierLength:
		return ErrEventIDTooLong
	}
	return nil
}

// checkAmount refuses what the payments domain already refuses at the source, because this
// value did not come from that domain: it came off a topic.
func checkAmount(minor int64) error {
	switch {
	case minor <= 0:
		return ErrAmountNotPositive
	case minor > maxAuthorizedMinor:
		return ErrAmountOutOfBalance
	}
	return nil
}
