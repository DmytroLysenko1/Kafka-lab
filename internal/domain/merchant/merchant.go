// Package merchant is the merchants' side of the payments: what a merchant has been
// authorised, as the consumer counts it. Its values arrive over a topic from a producer at
// any version, so every invariant is checked where the value is made, and a value that
// exists is one that can be stored and counted.
package merchant

import (
	"errors"
	"strings"
	"unicode/utf8"
)

var (
	ErrIDRequired        = errors.New("merchant: the merchant id is required")
	ErrIDTooLong         = errors.New("merchant: the merchant id is longer than any real one")
	ErrIDInvalid         = errors.New("merchant: the merchant id is not valid UTF-8 text")
	ErrEventIDRequired   = errors.New("merchant: the authorisation carries no event id to deduplicate it by")
	ErrEventIDTooLong    = errors.New("merchant: the event id is longer than any real one")
	ErrEventIDInvalid    = errors.New("merchant: the event id is not valid UTF-8 text")
	ErrAmountNotPositive = errors.New("merchant: an amount to count is positive")
	ErrAmountTooLarge    = errors.New("merchant: the amount is larger than one authorisation may be")
	ErrCurrencyFormat    = errors.New("merchant: the currency is not three upper-case letters")
	ErrTotalNegative     = errors.New("merchant: a total cannot be negative")
	ErrKindUnknown       = errors.New("merchant: the movement is not one a total counts")
	ErrStorage           = errors.New("merchant: storage failure")
)

// maxIdentifierLength matches what the payments domain allows a merchant id to be. Both
// identifiers become primary keys, and a producer at any version — or one that is not ours
// at all — can put a megabyte in a header, and a megabyte-wide key would be stored, indexed
// and kept forever. It is repeated here rather than imported: the payments context issues
// the ids and this one counts them, and neither should change when the other does.
const maxIdentifierLength = 64

// maxAuthorizedMinor is the payments domain's limit on one authorisation (2^50), repeated here
// because this value came off a topic, not from that domain. It keeps a single amount far from
// bigint's limit; a total that still reached it — eight thousand of the largest payments to one
// merchant in one currency — is refused by the database, and that event is dead-lettered as
// refused instead of stopping the consumer.
const maxAuthorizedMinor = 1 << 50

// currencyCodeLength is ISO 4217's. The check is repeated from the payments domain rather
// than imported, for the same reason as maxIdentifierLength.
const currencyCodeLength = 3

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
	case !isStorableText(value):
		return ID{}, ErrIDInvalid
	}
	return ID{
		value: value,
	}, nil
}

func (id ID) String() string {
	return id.value
}

// Currency is what a total is kept in. Amounts in different currencies are never added
// together: a merchant paid 1999 EUR and 1999 JPY has two totals, not 3998 of anything.
type Currency struct {
	code string
}

func ParseCurrency(code string) (Currency, error) {
	if len(code) != currencyCodeLength {
		return Currency{}, ErrCurrencyFormat
	}
	for _, letter := range code {
		if letter < 'A' || letter > 'Z' {
			return Currency{}, ErrCurrencyFormat
		}
	}
	return Currency{
		code: code,
	}, nil
}

func (c Currency) String() string {
	return c.code
}

// Amount is a sum in minor units of one currency. The two are checked together
// because neither means anything alone.
type Amount struct {
	minor    int64
	currency Currency
}

func NewAmount(minor int64, currency string) (Amount, error) {
	code, err := ParseCurrency(currency)
	if err != nil {
		return Amount{}, err
	}
	if err := checkAmount(minor); err != nil {
		return Amount{}, err
	}
	return Amount{
		minor:    minor,
		currency: code,
	}, nil
}

func (a Amount) Minor() int64 {
	return a.minor
}

func (a Amount) Currency() Currency {
	return a.currency
}

// Kind is what happened to the money a movement carries. A total keeps one sum per kind.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindAuthorized
	KindCaptured
	KindRefunded
)

func (k Kind) String() string {
	switch k {
	case KindAuthorized:
		return "authorized"
	case KindCaptured:
		return "captured"
	case KindRefunded:
		return "refunded"
	}
	return "unknown"
}

// Movement is one amount a merchant's totals are owed — authorised, captured or refunded —
// and the id of the event that announced it, the key a redelivery is recognised by.
type Movement struct {
	kind     Kind
	eventID  string
	merchant ID
	amount   Amount
}

func NewMovement(kind Kind, eventID, merchantID string, amount Amount) (Movement, error) {
	if kind == KindUnknown || kind > KindRefunded {
		return Movement{}, ErrKindUnknown
	}
	if err := checkEventID(eventID); err != nil {
		return Movement{}, err
	}
	merchant, err := ParseID(merchantID)
	if err != nil {
		return Movement{}, err
	}
	if amount == (Amount{}) {
		return Movement{}, ErrAmountNotPositive
	}
	return Movement{
		kind:     kind,
		eventID:  eventID,
		merchant: merchant,
		amount:   amount,
	}, nil
}

func (m Movement) Kind() Kind {
	return m.kind
}

func (m Movement) EventID() string {
	return m.eventID
}

func (m Movement) Merchant() ID {
	return m.merchant
}

func (m Movement) Amount() Amount {
	return m.amount
}

// Total is what a merchant has been authorised, captured and refunded in one currency, as the
// projection holds it. The three are not held to refunded <= captured <= authorized: the retry
// tiers may count a refund before the capture it follows, and a total on its way to the right
// sums passes through that order.
type Total struct {
	currency   Currency
	authorized int64
	captured   int64
	refunded   int64
}

// NewTotal rebuilds a total read back from the projection, which is not trusted any more
// than a topic is: a row that breaks what a total can be is refused rather than shown.
func NewTotal(currency string, authorized, captured, refunded int64) (Total, error) {
	code, err := ParseCurrency(currency)
	if err != nil {
		return Total{}, err
	}
	if authorized < 0 || captured < 0 || refunded < 0 {
		return Total{}, ErrTotalNegative
	}
	return Total{
		currency:   code,
		authorized: authorized,
		captured:   captured,
		refunded:   refunded,
	}, nil
}

func (t Total) Currency() Currency {
	return t.currency
}

func (t Total) Authorized() int64 {
	return t.authorized
}

func (t Total) Captured() int64 {
	return t.captured
}

func (t Total) Refunded() int64 {
	return t.refunded
}

func checkEventID(eventID string) error {
	switch {
	case eventID == "":
		return ErrEventIDRequired
	case len(eventID) > maxIdentifierLength:
		return ErrEventIDTooLong
	case !isStorableText(eventID):
		return ErrEventIDInvalid
	}
	return nil
}

// isStorableText refuses what Postgres refuses to store as text — a NUL byte and bytes that
// are not UTF-8. A header is whatever the producer put in it, and a key the database rejects
// on every delivery would stop the partition instead of reaching the dead letter topic.
func isStorableText(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

// checkAmount refuses what the payments domain already refuses at the source, because this
// value did not come from that domain: it came off a topic.
func checkAmount(minor int64) error {
	switch {
	case minor <= 0:
		return ErrAmountNotPositive
	case minor > maxAuthorizedMinor:
		return ErrAmountTooLarge
	}
	return nil
}
