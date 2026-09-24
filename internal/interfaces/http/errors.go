package http

import (
	"errors"
	"net/http"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

// failure is what a client sees. The code is a stable string it can branch on; the message
// is a fixed phrase, never the error itself — an error carries table names, identifiers and
// driver text, and a caller has no business learning any of it from a status page.
type failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

var refusals = []struct {
	sentinel error
	status   int
	code     string
	message  string
}{
	{payments.ErrIdempotencyKeyRequired, http.StatusBadRequest, "idempotency_key_required", "the Idempotency-Key header is required"},
	{payments.ErrIdempotencyKeyReused, http.StatusConflict, "idempotency_key_reused", "this Idempotency-Key was used for a different payment"},
	{payment.ErrMerchantRequired, http.StatusBadRequest, "merchant_required", "merchant_id is required"},
	{payment.ErrMerchantTooLong, http.StatusBadRequest, "merchant_too_long", "merchant_id is longer than allowed"},
	{payment.ErrCurrencyFormat, http.StatusBadRequest, "currency_invalid", "currency must be three upper-case letters"},
	{payment.ErrAmountNegative, http.StatusBadRequest, "amount_invalid", "amount_minor must be a positive number of minor units"},
	{payment.ErrAmountNotPositive, http.StatusBadRequest, "amount_invalid", "amount_minor must be a positive number of minor units"},
	{merchants.ErrUnprocessable, http.StatusBadRequest, "merchant_invalid", "the merchant id is not one this service can hold"},
	{payment.ErrStorage, http.StatusServiceUnavailable, "storage_unavailable", "the service cannot reach its storage"},
}

// refusalFor is the one place a domain error becomes a status code. Scattering this across
// handlers is how two endpoints end up answering the same refusal differently.
func refusalFor(err error) (int, failure) {
	for _, refusal := range refusals {
		if errors.Is(err, refusal.sentinel) {
			return refusal.status, failure{Code: refusal.code, Message: refusal.message}
		}
	}
	return http.StatusInternalServerError, failure{Code: "internal", Message: "the request could not be completed"}
}
