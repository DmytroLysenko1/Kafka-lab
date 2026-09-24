package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
)

type authorizer interface {
	Execute(ctx context.Context, cmd payments.AuthorizeCommand) (payments.AuthorizeResult, error)
}

type totalReader interface {
	Execute(ctx context.Context, merchantID string) (int64, error)
}

// authorizeRequest is the wire shape, and it stays here: the amount arrives as a number of
// minor units, never as a decimal string or a float, so nothing in this service ever has to
// decide what 19.99 rounds to.
type authorizeRequest struct {
	MerchantID  string `json:"merchant_id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

type authorizeResponse struct {
	PaymentID string `json:"payment_id"`
}

type totalResponse struct {
	MerchantID      string `json:"merchant_id"`
	AuthorizedMinor int64  `json:"authorized_minor"`
}

// authorizePayment answers 201 when it created the payment and 200 when the key had already
// bought one, so a client retrying after a timeout can tell whether it caused this payment
// or is looking at the one its earlier attempt made.
func (s *Server) authorizePayment(w http.ResponseWriter, r *http.Request) {
	var request authorizeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		s.respond(r.Context(), w, http.StatusBadRequest, failure{Code: "malformed_body", Message: "the request body is not the expected json"})
		return
	}

	result, err := s.authorize.Execute(r.Context(), payments.AuthorizeCommand{
		MerchantID:     request.MerchantID,
		AmountMinor:    request.AmountMinor,
		Currency:       request.Currency,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		s.refuse(r.Context(), w, "authorize a payment", err)
		return
	}

	status := http.StatusOK
	if result.Stored {
		status = http.StatusCreated
	}
	s.respond(r.Context(), w, status, authorizeResponse{PaymentID: result.PaymentID})
}

func (s *Server) merchantTotal(w http.ResponseWriter, r *http.Request) {
	merchantID := r.PathValue("merchantID")

	total, err := s.totals.Execute(r.Context(), merchantID)
	if err != nil {
		s.refuse(r.Context(), w, "read a merchant total", err)
		return
	}
	s.respond(r.Context(), w, http.StatusOK, totalResponse{MerchantID: merchantID, AuthorizedMinor: total})
}

// refuse is where the error finally stops: the layers below return it, this one logs it in
// full — once — and hands the caller a code that says what to do rather than what broke.
func (s *Server) refuse(ctx context.Context, w http.ResponseWriter, what string, err error) {
	status, body := refusalFor(err)
	if status >= http.StatusInternalServerError {
		s.logger.ErrorContext(ctx, "failed to "+what, "error", err)
	} else if errors.Is(err, context.Canceled) {
		s.logger.DebugContext(ctx, "the caller went away before "+what, "error", err)
	}
	s.respond(ctx, w, status, body)
}
