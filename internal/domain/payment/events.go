package payment

import "time"

// Event is sealed: only this package can add a payment event, so the adapter that maps
// events onto the wire can be exhaustive and stay exhaustive.
type Event interface {
	PaymentID() ID
	OccurredAt() time.Time
	sealedPaymentEvent()
}

type Authorized struct {
	paymentID  ID
	merchant   MerchantID
	amount     Money
	occurredAt time.Time
}

func (e *Authorized) PaymentID() ID {
	return e.paymentID
}

func (e *Authorized) Merchant() MerchantID {
	return e.merchant
}

func (e *Authorized) Amount() Money {
	return e.amount
}

func (e *Authorized) OccurredAt() time.Time {
	return e.occurredAt
}

// sealedPaymentEvent has no body on purpose and must not be removed: it is the unexported
// method that seals Event, so no type outside this package can be a payment event.
func (e *Authorized) sealedPaymentEvent() {
}
