package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	payhttp "github.com/DmytroLysenko1/Kafka-lab/internal/interfaces/http"
)

const (
	paymentPath = "/merchants/m-42/payments/11111111-1111-4111-8111-111111111111"
	paymentID   = "11111111-1111-4111-8111-111111111111"
)

type fakeChanger struct {
	result payments.ChangeResult
	err    error
	calls  int
	seen   payments.ChangeCommand
}

func (f *fakeChanger) Execute(_ context.Context, cmd payments.ChangeCommand) (payments.ChangeResult, error) {
	f.calls++
	f.seen = cmd
	return f.result, f.err
}

func changeServer(t *testing.T, capture, refund *fakeChanger) http.Handler {
	t.Helper()
	return serverWithChanges(t, capture, refund)
}

type changeExecutor interface {
	Execute(ctx context.Context, cmd payments.ChangeCommand) (payments.ChangeResult, error)
}

func changeServerFor(t *testing.T, capture changeExecutor) http.Handler {
	t.Helper()
	return serverWithChanges(t, capture, &fakeChanger{})
}

func serverWithChanges(t *testing.T, capture, refund changeExecutor) http.Handler {
	t.Helper()
	server, err := payhttp.NewServer(payhttp.UseCases{
		Authorize: &fakeAuthorizer{},
		Capture:   capture,
		Refund:    refund,
		Totals:    &fakeTotals{},
	}, &fakeDatabase{}, apiKey, slog.New(slog.DiscardHandler), &watcher{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return server.Handler()
}

func changeRequest(t *testing.T, path, key string) *http.Request {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, http.NoBody)
	request.Header.Set("X-API-Key", key)
	return request
}

// Each move reaches its own use case with the merchant and the payment taken from the path,
// and answers with the status the payment is in — 200 whether the move happened now or
// before, since a repeated capture is the same capture.
func TestAMoveReachesItsUseCaseWithThePathsMerchantAndPayment(t *testing.T) {
	type want struct {
		Status        int
		Body          map[string]string
		CaptureCalled int
		RefundCalled  int
		Seen          payments.ChangeCommand
	}
	tests := []struct {
		name string
		args string
		want want
	}{
		{
			name: "capture",
			args: paymentPath + "/capture",
			want: want{Status: http.StatusOK, Body: map[string]string{"payment_id": paymentID, "status": "captured"}, CaptureCalled: 1,
				Seen: payments.ChangeCommand{MerchantID: "m-42", PaymentID: paymentID}},
		},
		{
			name: "refund",
			args: paymentPath + "/refund",
			want: want{Status: http.StatusOK, Body: map[string]string{"payment_id": paymentID, "status": "refunded"}, RefundCalled: 1,
				Seen: payments.ChangeCommand{MerchantID: "m-42", PaymentID: paymentID}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capture := &fakeChanger{result: payments.ChangeResult{PaymentID: paymentID, Status: "captured"}}
			refund := &fakeChanger{result: payments.ChangeResult{PaymentID: paymentID, Status: "refunded"}}
			response := httptest.NewRecorder()

			changeServer(t, capture, refund).ServeHTTP(response, changeRequest(t, tt.args, apiKey))

			var body map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode %q: %v", response.Body.String(), err)
			}
			seen := capture.seen
			if refund.calls > 0 {
				seen = refund.seen
			}
			got := want{Status: response.Code, Body: body, CaptureCalled: capture.calls, RefundCalled: refund.calls, Seen: seen}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("response (-want +got):\n%s", diff)
			}
		})
	}
}

// A refused move says what the caller can do about it, in a code it can branch on; another
// merchant's payment is not found rather than forbidden, so its id tells nothing about it.
func TestARefusedMoveAnswersWithWhatTheCallerCanDo(t *testing.T) {
	type want struct {
		Status int
		Code   string
	}
	tests := []struct {
		name string
		args error
		want want
	}{
		{name: "no such payment for this merchant", args: payment.ErrNotFound, want: want{Status: http.StatusNotFound, Code: "payment_not_found"}},
		{name: "a refund of an authorisation", args: payment.ErrInvalidTransition, want: want{Status: http.StatusConflict, Code: "invalid_transition"}},
		{name: "the row changed under the lock", args: payment.ErrConcurrentModification, want: want{Status: http.StatusConflict, Code: "payment_changed"}},
		{name: "the commit's answer never arrived", args: payment.ErrOutcomeUnknown, want: want{Status: http.StatusServiceUnavailable, Code: "outcome_unknown"}},
		{name: "the database is down", args: payment.ErrStorage, want: want{Status: http.StatusServiceUnavailable, Code: "storage_unavailable"}},
		{name: "a stored payment the domain cannot read", args: payment.ErrCorrupt, want: want{Status: http.StatusInternalServerError, Code: "internal"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capture := &fakeChanger{err: tt.args}
			response := httptest.NewRecorder()

			changeServer(t, capture, &fakeChanger{}).ServeHTTP(response, changeRequest(t, paymentPath+"/capture", apiKey))

			if diff := cmp.Diff(tt.want, want{Status: response.Code, Code: decodeFailure(t, response)}); diff != "" {
				t.Errorf("refusal (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAMoveWithoutTheAPIKeyNeverReachesTheUseCase(t *testing.T) {
	capture := &fakeChanger{}
	response := httptest.NewRecorder()

	changeServer(t, capture, &fakeChanger{}).ServeHTTP(response, changeRequest(t, paymentPath+"/capture", "wrong-key"))

	if diff := cmp.Diff(http.StatusUnauthorized, response.Code); diff != "" {
		t.Errorf("status (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(0, capture.calls); diff != "" {
		t.Errorf("use case calls (-want +got):\n%s", diff)
	}
}

func TestAServerMissingAUseCaseRefusesToStart(t *testing.T) {
	_, err := payhttp.NewServer(payhttp.UseCases{
		Authorize: &fakeAuthorizer{},
		Capture:   &fakeChanger{},
		Totals:    &fakeTotals{},
	}, &fakeDatabase{}, apiKey, slog.New(slog.DiscardHandler), &watcher{})
	if !errors.Is(err, payhttp.ErrUseCaseRequired) {
		t.Fatalf("err = %v, want %v", err, payhttp.ErrUseCaseRequired)
	}
}
