package http

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

// failure is what a client sees. The code is a stable string it can branch on; the message
// is a fixed phrase, never the error itself — an error carries table names, identifiers and
// driver text, and a caller has no business learning any of it from a status page.
type failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// errMalformedBody is a body that is not the json this service reads: the one refusal that is
// the transport's own rather than a use case's.
var errMalformedBody = errors.New("http: the request body is not the expected json")

// refusal is one row of the table below: the sentinel a refusal is recognised by, and what the
// caller is told.
type refusal struct {
	sentinel error
	status   int
	code     string
	message  string
}

// refusals is every refusal a caller can act on, in the order they are tried: the first
// sentinel the error matches decides.
func refusals() []refusal {
	return []refusal{
		{errMalformedBody, http.StatusBadRequest, "malformed_body", "the request body is not the expected json"},
		{payments.ErrIdempotencyKeyRequired, http.StatusBadRequest, "idempotency_key_required", "the Idempotency-Key header is required"},
		{payments.ErrIdempotencyKeyTooLong, http.StatusBadRequest, "idempotency_key_too_long", "the Idempotency-Key header is longer than " + strconv.Itoa(payments.MaxIdempotencyKeyLength) + " bytes"},
		{payments.ErrIdempotencyKeyInvalid, http.StatusBadRequest, "idempotency_key_invalid", "the Idempotency-Key header is not UTF-8 text"},
		{payments.ErrIdempotencyKeyReused, http.StatusConflict, "idempotency_key_reused", "this Idempotency-Key was used for a different payment"},
		{payment.ErrMerchantRequired, http.StatusBadRequest, "merchant_required", "merchant_id is required"},
		{payment.ErrMerchantTooLong, http.StatusBadRequest, "merchant_too_long", "merchant_id is longer than allowed"},
		{payment.ErrCurrencyFormat, http.StatusBadRequest, "currency_invalid", "currency must be three upper-case letters"},
		{payment.ErrAmountNegative, http.StatusBadRequest, "amount_invalid", "amount_minor must be a positive number of minor units"},
		{payment.ErrAmountNotPositive, http.StatusBadRequest, "amount_invalid", "amount_minor must be a positive number of minor units"},
		{merchant.ErrIDRequired, http.StatusBadRequest, "merchant_invalid", "the merchant id is not one this service can hold"},
		{merchant.ErrIDTooLong, http.StatusBadRequest, "merchant_invalid", "the merchant id is not one this service can hold"},
		{payment.ErrAmountTooLarge, http.StatusBadRequest, "amount_too_large", "amount_minor is larger than one payment may be"},
		{payment.ErrMerchantInvalid, http.StatusBadRequest, "merchant_invalid", "the merchant id is not one this service can hold"},
		{merchant.ErrIDInvalid, http.StatusBadRequest, "merchant_invalid", "the merchant id is not one this service can hold"},
		{payment.ErrNotFound, http.StatusNotFound, "payment_not_found", "this merchant has no payment with that id"},
		{payment.ErrInvalidTransition, http.StatusConflict, "invalid_transition", "the payment is not in a state that allows this; only an authorised payment can be captured and only a captured one refunded"},
		{payment.ErrConcurrentModification, http.StatusConflict, "payment_changed", "the payment changed while this request was handled; repeat the request"},
		{payment.ErrOutcomeUnknown, http.StatusServiceUnavailable, "outcome_unknown", "the change may have been stored; repeat the request to find out — an authorisation with the same Idempotency-Key, a capture or refund as it was: each is applied once"},
		{payment.ErrStorage, http.StatusServiceUnavailable, "storage_unavailable", "the service cannot reach its storage; repeat the request — an authorisation with the same Idempotency-Key, a capture or refund as it was"},
		{merchant.ErrStorage, http.StatusServiceUnavailable, "storage_unavailable", "the service cannot reach its storage"},
		// A deadline or a caller gone inside a transaction reaches this table wrapped as
		// payment.ErrStorage and answers storage_unavailable: from here a slow database and an
		// exhausted pool look the same, and "repeat with the same key" is right for both. This row
		// is for a deadline that arrives outside any store.
		{context.DeadlineExceeded, http.StatusServiceUnavailable, "timeout", "the request did not finish in time; repeat it — an authorisation with the same Idempotency-Key, a capture or refund as it was"},
	}
}

// refusalFor is the one place a domain error becomes a status code. Scattering this across
// handlers is how two endpoints end up answering the same refusal differently.
func refusalFor(err error) (int, failure) {
	for _, refusal := range refusals() {
		if errors.Is(err, refusal.sentinel) {
			return refusal.status, failure{
				Code:    refusal.code,
				Message: refusal.message,
			}
		}
	}
	return http.StatusInternalServerError, failure{
		Code:    "internal",
		Message: "the request could not be completed",
	}
}
