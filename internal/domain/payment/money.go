package payment

import "errors"

var (
	ErrCurrencyFormat = errors.New("payment: currency must be three upper-case letters")
	ErrAmountNegative = errors.New("payment: amount cannot be negative")
)

const currencyCodeLength = 3

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

// Money is minor units — cents, kopiykas — and never a float: exp-17's payloads are not the
// only thing that has to survive a round trip.
type Money struct {
	minor    int64
	currency Currency
}

func NewMoney(minor int64, currency Currency) (Money, error) {
	if minor < 0 {
		return Money{}, ErrAmountNegative
	}
	if currency == (Currency{}) {
		return Money{}, ErrCurrencyFormat
	}
	return Money{
		minor:    minor,
		currency: currency,
	}, nil
}

func (m Money) Minor() int64 {
	return m.minor
}

// IsPositive is whether this is an amount that moves anything: Money allows zero, because a
// balance can be zero, but zero authorises nothing.
func (m Money) IsPositive() bool {
	return m.minor > 0
}

func (m Money) Currency() Currency {
	return m.currency
}
