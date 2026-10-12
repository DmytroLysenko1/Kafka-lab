package payment

import "time"

// Event is sealed: only this package can add a payment event, so the adapter that maps
// events onto the wire can be exhaustive and stay exhaustive.
type Event interface {
	PaymentID() ID
	Merchant() MerchantID
	Amount() Money
	OccurredAt() time.Time
	sealedPaymentEvent()
}

// movement is what every payment event says: which payment, whose, for how much and when.
// The event types differ in what happened, not in what they carry.
type movement struct {
	paymentID  ID
	merchant   MerchantID
	amount     Money
	occurredAt time.Time
}

func (p *Payment) movement(at time.Time) movement {
	return movement{
		paymentID:  p.id,
		merchant:   p.merchant,
		amount:     p.amount,
		occurredAt: at,
	}
}

// Pointer receivers on a value that never changes: the adapter tells event types apart with
// a type switch on *Authorized, *Captured and *Refunded (postgres/events.go), and one receiver
// kind keeps that the only shape an event travels in — a value copy would satisfy Event just as
// well and slip past it.
func (m *movement) PaymentID() ID {
	return m.paymentID
}

func (m *movement) Merchant() MerchantID {
	return m.merchant
}

func (m *movement) Amount() Money {
	return m.amount
}

func (m *movement) OccurredAt() time.Time {
	return m.occurredAt
}

type Authorized struct {
	movement
}

type Captured struct {
	movement
}

type Refunded struct {
	movement
}

// sealedPaymentEvent has no body on purpose and must not be removed: it is the unexported
// method that seals Event, so no type outside this package can be a payment event. It is
// declared on each event rather than on movement, so that movement alone is not an Event.
func (e *Authorized) sealedPaymentEvent() {
}

func (e *Captured) sealedPaymentEvent() {
}

func (e *Refunded) sealedPaymentEvent() {
}
