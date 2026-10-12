package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
)

type authorizer interface {
	Execute(ctx context.Context, cmd payments.AuthorizeCommand) (payments.AuthorizeResult, error)
}

type changer interface {
	Execute(ctx context.Context, cmd payments.ChangeCommand) (payments.ChangeResult, error)
}

type totalReader interface {
	Execute(ctx context.Context, merchantID string) ([]merchant.Total, error)
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

type changeResponse struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
}

// totalResponse lists one total per currency: amounts in different currencies are never
// summed into one number.
type totalResponse struct {
	MerchantID string          `json:"merchant_id"`
	Totals     []currencyTotal `json:"totals"`
}

// currencyTotal keeps what was authorised, captured and refunded apart: they are three
// different questions, and their difference is what is still held or kept.
type currencyTotal struct {
	Currency        string `json:"currency"`
	AuthorizedMinor int64  `json:"authorized_minor"`
	CapturedMinor   int64  `json:"captured_minor"`
	RefundedMinor   int64  `json:"refunded_minor"`
}

// authorizePayment answers 201 when it created the payment and 200 when the key had already
// bought one, so a client retrying after a timeout can tell whether it caused this payment
// or is looking at the one its earlier attempt made.
func (s *Server) authorizePayment(w http.ResponseWriter, r *http.Request) {
	command, err := readAuthorizeCommand(w, r)
	if err != nil {
		s.refuse(r.Context(), w, "authorize a payment", err)
		return
	}

	result, err := s.useCases.Authorize.Execute(r.Context(), command)
	if err != nil {
		s.refuse(r.Context(), w, "authorize a payment", err)
		return
	}

	status := http.StatusOK
	if result.Stored {
		status = http.StatusCreated
	}
	s.respond(r.Context(), w, status, authorizeResponse{
		PaymentID: result.PaymentID,
	})
}

// readAuthorizeCommand takes the command off the wire, or says why the request is not one: a
// body that is not the expected json is a malformed request; a key the store could not hold is
// refused in the use case's words, mapped once in errors.go like every other refusal.
func readAuthorizeCommand(w http.ResponseWriter, r *http.Request) (payments.AuthorizeCommand, error) {
	var request authorizeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return payments.AuthorizeCommand{}, errors.Join(errMalformedBody, err)
	}

	idempotencyKey, err := payments.ParseIdempotencyKey(r.Header.Get("Idempotency-Key"))
	if err != nil {
		return payments.AuthorizeCommand{}, err
	}
	return payments.AuthorizeCommand{
		MerchantID:     request.MerchantID,
		AmountMinor:    request.AmountMinor,
		Currency:       request.Currency,
		IdempotencyKey: idempotencyKey,
	}, nil
}

// changePayment answers 200 whether the move happened now or had happened before: a capture
// repeated after its answer was lost is the same capture, and the caller is told the status
// the payment is in either way. The body is empty — the payment and its merchant are the path.
func (s *Server) changePayment(what string, change changer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result, err := change.Execute(r.Context(), payments.ChangeCommand{
			MerchantID: r.PathValue("merchantID"),
			PaymentID:  r.PathValue("paymentID"),
		})
		if err != nil {
			s.refuse(r.Context(), w, what, err)
			return
		}
		s.respond(r.Context(), w, http.StatusOK, changeResponse{
			PaymentID: result.PaymentID,
			Status:    result.Status,
		})
	})
}

func (s *Server) merchantTotal(w http.ResponseWriter, r *http.Request) {
	merchantID := r.PathValue("merchantID")

	totals, err := s.useCases.Totals.Execute(r.Context(), merchantID)
	if err != nil {
		s.refuse(r.Context(), w, "read a merchant total", err)
		return
	}
	s.respond(r.Context(), w, http.StatusOK, totalResponse{
		MerchantID: merchantID,
		Totals: lo.Map(totals, func(total merchant.Total, _ int) currencyTotal {
			return currencyTotal{
				Currency:        total.Currency().String(),
				AuthorizedMinor: total.Authorized(),
				CapturedMinor:   total.Captured(),
				RefundedMinor:   total.Refunded(),
			}
		}),
	})
}

// refuse is where the error finally stops: the layers below return it, this one logs it in
// full — once — and hands the caller a code that says what to do rather than what broke.
func (s *Server) refuse(ctx context.Context, w http.ResponseWriter, what string, err error) {
	status, body := refusalFor(err)
	switch {
	case errors.Is(err, payment.ErrOutcomeUnknown):
		// Never lowered to Debug, whoever went away: a payment that may or may not be kept is
		// the one failure somebody may have to reconcile by hand.
		s.logger.ErrorContext(ctx, "request outcome unknown", "operation", what, "error", err)
	case errors.Is(err, context.Canceled):
		s.logger.DebugContext(ctx, "caller went away", "operation", what, "error", err)
	case status >= http.StatusInternalServerError:
		s.logger.ErrorContext(ctx, "request failed", "operation", what, "error", err)
	}
	s.respond(ctx, w, status, body)
}
